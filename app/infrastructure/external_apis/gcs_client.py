"""Google Cloud Storage client for image uploads and processing."""
import logging
from typing import Optional, Tuple
from io import BytesIO

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from PIL import Image

from app.config import settings

logger = logging.getLogger(__name__)


class GCSImageClient:
    """Client for uploading and managing images in Google Cloud Storage."""

    def __init__(self):
        self.bucket_name = settings.GCS_BUCKET_NAME
        self.cdn_url = settings.GCS_CDN_URL
        self._client: Optional[storage.Client] = None
        self._bucket: Optional[storage.Bucket] = None

    @property
    def client(self) -> storage.Client:
        """Lazy initialization of GCS client."""
        if self._client is None:
            import os
            creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_path and os.path.exists(creds_path):
                self._client = storage.Client.from_service_account_json(
                    creds_path,
                    project=settings.GCS_PROJECT_ID or None
                )
            elif settings.GCS_PROJECT_ID:
                self._client = storage.Client(project=settings.GCS_PROJECT_ID)
            else:
                # Use default credentials
                self._client = storage.Client()
        return self._client

    @property
    def bucket(self) -> storage.Bucket:
        """Lazy initialization of bucket."""
        if self._bucket is None:
            self._bucket = self.client.bucket(self.bucket_name)
        return self._bucket

    def upload_image(
        self,
        image_bytes: bytes,
        blob_path: str,
        content_type: str = "image/webp"
    ) -> Optional[str]:
        """Upload image to GCS bucket.

        Args:
            image_bytes: Raw image bytes
            blob_path: Path within bucket (e.g., 'attractions/123/hero_1.webp')
            content_type: MIME type

        Returns:
            CDN URL or None if upload failed
        """
        try:
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(image_bytes, content_type=content_type)
            # Set cache control for CDN
            blob.cache_control = "public, max-age=31536000"  # 1 year cache
            blob.patch()

            cdn_url = f"{self.cdn_url}/{blob_path}"
            logger.info(f"Uploaded image to GCS: {cdn_url}")
            return cdn_url

        except GoogleCloudError as e:
            logger.error(f"GCS upload failed for {blob_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error uploading to GCS: {e}")
            return None

    def delete_image(self, blob_path: str) -> bool:
        """Delete image from GCS bucket.

        Args:
            blob_path: Path within bucket

        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            blob = self.bucket.blob(blob_path)
            blob.delete()
            logger.info(f"Deleted image from GCS: {blob_path}")
            return True
        except GoogleCloudError as e:
            logger.error(f"GCS delete failed for {blob_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error deleting from GCS: {e}")
            return False

    def image_exists(self, blob_path: str) -> bool:
        """Check if image exists in GCS bucket.

        Args:
            blob_path: Path within bucket

        Returns:
            True if exists, False otherwise
        """
        try:
            blob = self.bucket.blob(blob_path)
            return blob.exists()
        except GoogleCloudError as e:
            logger.error(f"GCS existence check failed for {blob_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking GCS existence: {e}")
            return False

    def get_blob_url(self, blob_path: str) -> str:
        """Get CDN URL for a blob path.

        Args:
            blob_path: Path within bucket

        Returns:
            Full CDN URL
        """
        return f"{self.cdn_url}/{blob_path}"


class ImageProcessor:
    """Process images: resize, convert to WebP."""

    @staticmethod
    def process_image(
        image_bytes: bytes,
        target_width: int,
        quality: int = 85
    ) -> Tuple[bytes, int, int]:
        """Resize and convert image to WebP.

        Args:
            image_bytes: Raw image bytes (any supported format)
            target_width: Target width in pixels
            quality: WebP quality (1-100)

        Returns:
            Tuple of (webp_bytes, width, height)

        Raises:
            ValueError: If image cannot be processed
        """
        try:
            img = Image.open(BytesIO(image_bytes))

            # Convert to RGB if necessary (for PNG with transparency, CMYK, etc.)
            if img.mode in ('RGBA', 'P', 'LA'):
                # Create white background for transparency
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')

            # Calculate new height maintaining aspect ratio
            original_width, original_height = img.size

            # Only resize if the image is larger than target
            if original_width > target_width:
                ratio = target_width / original_width
                target_height = int(original_height * ratio)
                # Resize using high-quality resampling
                img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
            else:
                target_width = original_width
                target_height = original_height

            # Convert to WebP
            output = BytesIO()
            img.save(output, format='WEBP', quality=quality, method=6)  # method=6 for best compression
            webp_bytes = output.getvalue()

            return webp_bytes, target_width, target_height

        except Exception as e:
            logger.error(f"Error processing image: {e}")
            raise ValueError(f"Failed to process image: {e}")

    @staticmethod
    def get_image_dimensions(image_bytes: bytes) -> Tuple[int, int]:
        """Get dimensions of an image.

        Args:
            image_bytes: Raw image bytes

        Returns:
            Tuple of (width, height)
        """
        img = Image.open(BytesIO(image_bytes))
        return img.size


# Global instances for easy import
gcs_client = GCSImageClient()
image_processor = ImageProcessor()
