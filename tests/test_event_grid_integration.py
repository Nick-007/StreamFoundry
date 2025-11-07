import json
import os
import subprocess
import time
import unittest
from pathlib import Path
from azure.storage.queue import QueueClient

# Ensure queues exist
ROOT = Path(__file__).resolve().parent.parent
subprocess.run([sys.executable, str(ROOT / "infra/local/seed-queues.py")], check=True)

# Load settings from local.settings.json
with open(ROOT / "local.settings.json") as f:
    settings = json.load(f).get("Values", {})
    for key, value in settings.items():
        os.environ.setdefault(key, str(value))

class EventGridIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Path to the test script
        cls.test_script = Path(__file__).parent / "test_event_grid_trigger.sh"
        if not cls.test_script.exists():
            raise FileNotFoundError(f"Test script not found: {cls.test_script}")
        
        # Make sure script is executable
        cls.test_script.chmod(0o755)
        
        # Get queue connection string from environment
        connection_string = os.getenv("AzureWebJobsStorage", 
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
            "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;" +
            "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;" +
            "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1"
        )
        
        # Initialize queue client for checking results
        cls.queue_client = QueueClient.from_connection_string(
            connection_string,
            "transcode-jobs"
        )

    def setUp(self):
        # Clear any existing messages from the queue
        while True:
            messages = self.queue_client.receive_messages()
            if not list(messages):
                break
            for msg in messages:
                self.queue_client.delete_message(msg)

    def test_blob_created_event_triggers_transcode_job(self):
        """Test that a blob creation event triggers a transcode job."""
        test_blob = "test_video.mp4"
        
        # Send the Event Grid notification
        result = subprocess.run([
            str(self.test_script),
            "--blob", test_blob,
            "--type", "video/mp4",
            "--length", "1048576"  # 1MB
        ], capture_output=True, text=True)
        
        # Check that the script executed successfully
        self.assertEqual(result.returncode, 0, 
            f"Script failed with:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
        
        # Wait for message to appear in queue (up to 5 seconds)
        message = None
        for _ in range(10):
            messages = self.queue_client.receive_messages()
            message = next(iter(messages), None)
            if message:
                break
            time.sleep(0.5)
        
        # Assert that we got a message
        self.assertIsNotNone(message, "No message received in transcode queue")
        
        # Parse and validate the message content
        content = json.loads(message.content)
        self.assertEqual(content["id"], "test_video")
        self.assertEqual(content["in"]["container"], "raw")
        self.assertEqual(content["in"]["key"], test_blob)
        self.assertEqual(content["extra"]["pipeline"], "transcode")
        self.assertEqual(content["captions"], [])

    def test_ignores_non_video_content(self):
        """Test that non-video content doesn't trigger a transcode job."""
        test_blob = "test_document.pdf"
        
        # Send the Event Grid notification
        result = subprocess.run([
            str(self.test_script),
            "--blob", test_blob,
            "--type", "application/pdf",
            "--length", "1024"
        ], capture_output=True, text=True)
        
        # Check that the script executed successfully
        self.assertEqual(result.returncode, 0)
        
        # Wait briefly to ensure no message appears
        time.sleep(2)
        
        # Check that no message was enqueued
        messages = self.queue_client.receive_messages()
        self.assertEqual(len(list(messages)), 0, 
            "Received unexpected message for non-video content")

    def test_ignores_wrong_container(self):
        """Test that blobs in other containers are ignored."""
        test_blob = "test_video.mp4"
        
        # Send the Event Grid notification
        result = subprocess.run([
            str(self.test_script),
            "--container", "wrong-container",
            "--blob", test_blob,
            "--type", "video/mp4",
            "--length", "1024"
        ], capture_output=True, text=True)
        
        # Check that the script executed successfully
        self.assertEqual(result.returncode, 0)
        
        # Wait briefly to ensure no message appears
        time.sleep(2)
        
        # Check that no message was enqueued
        messages = self.queue_client.receive_messages()
        self.assertEqual(len(list(messages)), 0, 
            "Received unexpected message for wrong container")

if __name__ == '__main__':
    unittest.main()