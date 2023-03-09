import asyncio
import motor.motor_asyncio
from bson.objectid import ObjectId
import logging

class MotorChunkUtil():
    """Class to test motor chunk read/write"""

    def __init__(self, url="mongodb://localhost:27017"):
        """db connection should be in global handler e.g. server.py before service startup
        later pass to handler initialize for access in requests."""

        try:
            self.client = motor.motor_asyncio.AsyncIOMotorClient(url)
            self.database = self.client["mydb"]
            self.bucket = motor.motor_asyncio.AsyncIOMotorGridFSBucket(self.database)
        except Exception as ex:
            logging.error("__init__: %s", ex)


    async def mock_stateless_request(self, filename: str, my_chunk, file_id=None):
        """Simulate a POST request"""

        try:
            if file_id is None:
                upload_stream = self.bucket.open_upload_stream(filename=filename)
                await upload_stream.write(my_chunk)
                file_id = upload_stream._id     
            else:
                upload_stream = self.bucket.open_upload_stream_with_id(ObjectId(file_id),filename=filename)
                await upload_stream.write(my_chunk)

            upload_stream.close()
        except Exception as ex:
            logging.error("mock_stateless_request: %s", ex)

        return file_id


    async def download_test_file(self, dest_path: str, file_id):
        """Test if file was stored properly, by writing it out."""

        try:
            file = await self.bucket.find_one({'_id': ObjectId(file_id)})
            with open(dest_path, 'wb') as dest_file:
                async for data in self.bucket.open_download_stream(file['_id']):
                    dest_file.write(data)
        except Exception as ex:
            logging.error("download_test_file: %s", ex)


    def close(self):
        """perform any exit/close client separately"""
        try:
            self.client.close()
        except Exception as ex:
            logging.error("close: %s", ex)
      

async def main():
    """instantiate util test class, read a file and write a file to verify."""

    chunk_size = 1024 * 1024
    input_file = "TestData10.5MB.pdf"
    output_test_file = "testresult.pdf"

    chunk_util = MotorChunkUtil()
    file_id = None

    with open(input_file, "rb") as file:
        logging.info("Open file to read success: %s", input_file)
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            file_id = await chunk_util.mock_stateless_request(input_file, chunk, file_id)
    
    if file_id is not None:
        logging.info("File storage completed with file id: %s, Retrieving test next...", file_id)
        await chunk_util.download_test_file(output_test_file, file_id)
        logging.info("Completed writing file to: %s", output_test_file)
    else:
        logging.error("File id is None")

    chunk_util.close()

    logging.info("Exiting")

    

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
