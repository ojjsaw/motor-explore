import asyncio
import logging
import minio

class Part():

    def __init__(self, part_number, etag):
        self.part_number = part_number
        self.etag = etag

class MinioChunkUtil():
    """Class to test motor chunk read/write"""

    def __init__(self, bucket_name: str, url="localhost:9000", ):
        """db connection should be in global handler e.g. server.py before service startup
        later pass to handler initialize for access in requests."""

        try:
            # generate key, secret from GUI console first (below is temp. expired)
            self.minio_client = minio.Minio(url,
                                            access_key="7jbustgFJp0X1vAS",
                                            secret_key="XsGKN79T58XINPtAbv8Gg0gYL0pRGGXF",
                                            secure=False)
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
        except Exception as ex:
            logging.error("__init__: %s", ex)


    def mock_stateless_request(self, bucket_name, object_name, my_chunk, part_number, upload_id=None):
        """Simulate a POST request"""

        headers = {"x-amz-acl": "public-read"}

        try:
            if upload_id is None:
                upload_id = self.minio_client._create_multipart_upload(bucket_name= bucket_name,
                                                                             object_name= object_name,
                                                                             headers=headers)
            
            part = self.minio_client._upload_part(
                bucket_name= bucket_name,
                object_name= object_name,
                part_number= part_number,
                upload_id= upload_id,
                data= my_chunk,
                headers=headers
            )

        except Exception as ex:
            logging.error("mock_stateless_request: %s", ex)

        return upload_id, part

    def complete_upload(self, bucket_name, object_name, upload_id, parts):
        """Call once done"""
        self.minio_client._complete_multipart_upload(bucket_name= bucket_name, 
                                                           object_name= object_name,
                                                           upload_id= upload_id,
                                                           parts=parts)

    def download_test_file(self, bucket_name, object_name, destination_file: str):
        """Test if file was stored properly, by writing it out."""

        try:
            self.minio_client.fget_object(bucket_name, object_name, destination_file)
        except Exception as ex:
            logging.error("download_test_file: %s", ex)


    def close(self):
        """perform any exit/close client separately"""
        try:
            pass
        except Exception as ex:
            logging.error("close: %s", ex)
    

async def main():
    """instantiate util test class, read a file and write a file to verify."""

    # default minimum part upload size 5MB (need to check custom policy for smaller size)
    chunk_size = 5 * 1024 * 1024
    input_file = "TestData10.5MB.pdf"
    output_test_file = "testresult.pdf"

    bucket_name = "mybucket"
    object_name = input_file

    chunk_util = MinioChunkUtil(bucket_name)

    with open(input_file, "rb") as file:
        logging.info("Open file to read success: %s", input_file)

        upload_id = None
        parts = []
        part_number = 1

        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                chunk_util.complete_upload(bucket_name, object_name, upload_id, parts)
                break

            # Calculate the part number based on the chunk position
            upload_id, etag = chunk_util.mock_stateless_request(bucket_name,
                                                                object_name,
                                                                chunk,
                                                                part_number,
                                                                upload_id)

            part = Part(part_number, etag)
            parts.append(part)
            part_number = part_number + 1

    if upload_id is not None:
        logging.info("File storage completed with file id: %s, Retrieving test next...", upload_id)
        chunk_util.download_test_file(bucket_name, object_name, output_test_file)
        logging.info("Completed writing file to: %s", output_test_file)
    else:
        logging.error("File id is None")

    chunk_util.close()

    logging.info("Exiting")

    

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
