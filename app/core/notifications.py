"""Notification system for sending alerts via Slack and email."""
import os
import json
import smtplib
from enum import Enum
from typing import Optional, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class AlertType(Enum):
    """Types of alerts that can be sent."""
    QUOTA_EXCEEDED = "quota_exceeded"
    PIPELINE_FAILED = "pipeline_failed"
    DATABASE_ERROR = "database_error"
    BACKUP_FAILED = "backup_failed"
    API_ERROR = "api_error"
    SYSTEM_ERROR = "system_error"


class AlertSeverity(Enum):
    """Severity levels for alerts."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class NotificationManager:
    """Manages sending notifications via Slack and email."""
    
    def __init__(self):
        """Initialize notification manager with configuration from environment."""
        # Slack configuration
        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        self.slack_enabled = bool(self.slack_webhook_url)
        
        # Email configuration
        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.smtp_from_email = os.getenv("SMTP_FROM_EMAIL")
        self.smtp_to_emails = os.getenv("SMTP_TO_EMAILS", "").split(",")
        self.smtp_to_emails = [email.strip() for email in self.smtp_to_emails if email.strip()]
        self.email_enabled = bool(
            self.smtp_host and 
            self.smtp_username and 
            self.smtp_password and 
            self.smtp_from_email and 
            self.smtp_to_emails
        )
        
        # Notification settings
        self.notifications_enabled = os.getenv("NOTIFICATIONS_ENABLED", "true").lower() == "true"
        
        if self.notifications_enabled:
            if self.slack_enabled:
                logger.info("Slack notifications enabled")
            if self.email_enabled:
                logger.info(f"Email notifications enabled for {len(self.smtp_to_emails)} recipients")
            if not self.slack_enabled and not self.email_enabled:
                logger.warning("Notifications enabled but no channels configured")
    
    def send_alert(
        self,
        alert_type: AlertType,
        severity: AlertSeverity,
        title: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send an alert notification via configured channels.
        
        Args:
            alert_type: Type of alert being sent
            severity: Severity level of the alert
            title: Short title for the alert
            message: Detailed message describing the alert
            metadata: Optional additional data to include
            
        Returns:
            True if at least one notification was sent successfully
        """
        # Log alert to database first
        self._log_alert_to_database(alert_type, severity, title, message, metadata)
        
        if not self.notifications_enabled:
            logger.debug("Notifications disabled, skipping alert")
            return False
        
        success = False
        
        # Send to Slack
        if self.slack_enabled:
            try:
                if self._send_slack_notification(alert_type, severity, title, message, metadata):
                    success = True
            except Exception as e:
                logger.error(f"Failed to send Slack notification: {e}")
        
        # Send via email
        if self.email_enabled:
            try:
                if self._send_email_notification(alert_type, severity, title, message, metadata):
                    success = True
            except Exception as e:
                logger.error(f"Failed to send email notification: {e}")
        
        return success
    
    def _send_slack_notification(
        self,
        alert_type: AlertType,
        severity: AlertSeverity,
        title: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Send notification to Slack via webhook."""
        if not self.slack_webhook_url:
            return False
        
        # Map severity to Slack color
        color_map = {
            AlertSeverity.INFO: "#36a64f",      # green
            AlertSeverity.WARNING: "#ff9900",   # orange
            AlertSeverity.ERROR: "#ff0000",     # red
            AlertSeverity.CRITICAL: "#8b0000"   # dark red
        }
        
        # Build Slack message payload
        payload = {
            "attachments": [
                {
                    "color": color_map.get(severity, "#808080"),
                    "title": f"{severity.value.upper()}: {title}",
                    "text": message,
                    "fields": [
                        {
                            "title": "Alert Type",
                            "value": alert_type.value,
                            "short": True
                        },
                        {
                            "title": "Severity",
                            "value": severity.value,
                            "short": True
                        },
                        {
                            "title": "Timestamp",
                            "value": datetime.utcnow().isoformat(),
                            "short": False
                        }
                    ],
                    "footer": "Storyboard Alert System"
                }
            ]
        }
        
        # Add metadata fields if provided
        if metadata:
            for key, value in metadata.items():
                payload["attachments"][0]["fields"].append({
                    "title": key.replace("_", " ").title(),
                    "value": str(value),
                    "short": True
                })
        
        try:
            response = httpx.post(
                self.slack_webhook_url,
                json=payload,
                timeout=10.0
            )
            response.raise_for_status()
            logger.info(f"Slack notification sent: {alert_type.value}")
            return True
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    def _send_email_notification(
        self,
        alert_type: AlertType,
        severity: AlertSeverity,
        title: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Send notification via email."""
        if not self.email_enabled:
            return False
        
        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{severity.value.upper()}] {title}"
            msg["From"] = self.smtp_from_email
            msg["To"] = ", ".join(self.smtp_to_emails)
            
            # Build email body
            text_body = self._build_email_text(alert_type, severity, title, message, metadata)
            html_body = self._build_email_html(alert_type, severity, title, message, metadata)
            
            # Attach both plain text and HTML versions
            part1 = MIMEText(text_body, "plain")
            part2 = MIMEText(html_body, "html")
            msg.attach(part1)
            msg.attach(part2)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email notification sent to {len(self.smtp_to_emails)} recipients: {alert_type.value}")
            return True
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    def _build_email_text(
        self,
        alert_type: AlertType,
        severity: AlertSeverity,
        title: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Build plain text email body."""
        lines = [
            f"STORYBOARD ALERT",
            f"=" * 50,
            f"",
            f"Severity: {severity.value.upper()}",
            f"Alert Type: {alert_type.value}",
            f"Title: {title}",
            f"",
            f"Message:",
            f"{message}",
            f"",
            f"Timestamp: {datetime.utcnow().isoformat()}",
        ]
        
        if metadata:
            lines.append("")
            lines.append("Additional Information:")
            for key, value in metadata.items():
                lines.append(f"  {key.replace('_', ' ').title()}: {value}")
        
        return "\n".join(lines)
    
    def _build_email_html(
        self,
        alert_type: AlertType,
        severity: AlertSeverity,
        title: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Build HTML email body."""
        # Map severity to color
        color_map = {
            AlertSeverity.INFO: "#36a64f",
            AlertSeverity.WARNING: "#ff9900",
            AlertSeverity.ERROR: "#ff0000",
            AlertSeverity.CRITICAL: "#8b0000"
        }
        color = color_map.get(severity, "#808080")
        
        html = f"""
        <html>
          <head>
            <style>
              body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
              .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
              .header {{ background-color: {color}; color: white; padding: 15px; border-radius: 5px 5px 0 0; }}
              .content {{ background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; border-top: none; }}
              .field {{ margin: 10px 0; }}
              .field-label {{ font-weight: bold; color: #333; }}
              .field-value {{ color: #666; }}
              .metadata {{ background-color: #fff; padding: 15px; margin-top: 15px; border-left: 3px solid {color}; }}
              .footer {{ text-align: center; color: #999; margin-top: 20px; font-size: 12px; }}
            </style>
          </head>
          <body>
            <div class="container">
              <div class="header">
                <h2 style="margin: 0;">{severity.value.upper()}: {title}</h2>
              </div>
              <div class="content">
                <div class="field">
                  <span class="field-label">Alert Type:</span>
                  <span class="field-value">{alert_type.value}</span>
                </div>
                <div class="field">
                  <span class="field-label">Severity:</span>
                  <span class="field-value">{severity.value}</span>
                </div>
                <div class="field">
                  <span class="field-label">Timestamp:</span>
                  <span class="field-value">{datetime.utcnow().isoformat()}</span>
                </div>
                <div class="field" style="margin-top: 20px;">
                  <div class="field-label">Message:</div>
                  <div style="margin-top: 10px; white-space: pre-wrap;">{message}</div>
                </div>
        """
        
        if metadata:
            html += """
                <div class="metadata">
                  <div class="field-label">Additional Information:</div>
            """
            for key, value in metadata.items():
                html += f"""
                  <div class="field">
                    <span class="field-label">{key.replace('_', ' ').title()}:</span>
                    <span class="field-value">{value}</span>
                  </div>
                """
            html += """
                </div>
            """
        
        html += """
              </div>
              <div class="footer">
                <p>Storyboard Alert System</p>
              </div>
            </div>
          </body>
        </html>
        """
        
        return html
    
    def _log_alert_to_database(
        self,
        alert_type: AlertType,
        severity: AlertSeverity,
        title: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log alert to database for auditing."""
        try:
            import pymysql
            from datetime import datetime
            
            config = {
                'host': os.getenv('DATABASE_HOST', 'localhost'),
                'port': int(os.getenv('DATABASE_PORT', 3306)),
                'user': os.getenv('DATABASE_USER', 'root'),
                'password': os.getenv('DATABASE_PASSWORD', ''),
                'database': os.getenv('DATABASE_NAME', 'storyboard'),
                'charset': 'utf8mb4',
                'cursorclass': pymysql.cursors.DictCursor
            }
            
            conn = pymysql.connect(**config)
            try:
                with conn.cursor() as cursor:
                    # Convert metadata to JSON string
                    metadata_json = json.dumps(metadata) if metadata else None
                    
                    cursor.execute("""
                        INSERT INTO system_alerts 
                        (alert_type, severity, title, message, metadata, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        alert_type.value,
                        severity.value,
                        title,
                        message,
                        metadata_json,
                        datetime.utcnow()
                    ))
                    conn.commit()
                    logger.debug(f"Logged alert to database: {alert_type.value}")
            finally:
                conn.close()
        except Exception as e:
            # Don't fail the notification if database logging fails
            logger.error(f"Failed to log alert to database: {e}")


# Global notification manager instance
notification_manager = NotificationManager()
