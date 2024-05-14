"""Servant implementation for the delayed response mechanism."""

import os
import Ice
import IceDrive
import logging
from blob import DataTransfer, BlobService  

class BlobQueryResponse(IceDrive.BlobQueryResponse):
    """Query response receiver."""
    def downloadBlob(self, blob: IceDrive.DataTransferPrx, current: Ice.Current = None) -> None:
        """Receive a `DataTransfer` when other service instance knows the `blob_id`."""
        if current:
            logging.info("Received DataTransfer for blob download.")
            file_path = os.path.join("/tmp", blob.ice_getIdentity().name)
            data_transfer = DataTransfer(file_path)
            
            while True:
                try:
                    data = blob.read(4096)
                    if not data:
                        break
                    data_transfer.file.write(data)
                except IceDrive.FailedToReadData:
                    logging.error("Failed to read data from DataTransfer.")
                    break
            
            blob_id = BlobService().calculateBlobId(file_path)
            blob_service = BlobService()
            blob_service.blobs[blob_id] = {"file_path": file_path, "links": 1}
            logging.info(f"Blob {blob_id} downloaded and saved successfully.")
        else:
            raise Ice.UnknownException("No current object provided.")

    def blobExists(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and it's stored there."""
        if current:
            logging.info("Blob exists in the system.")
        else:
            raise Ice.UnknownException("No current object provided.")

    def blobLinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was linked."""
        if current:
            logging.info("Blob has been successfully linked.")
        else:
            raise Ice.UnknownException("No current object provided.")

    def blobUnlinked(self, current: Ice.Current = None) -> None:
        """Indicate that `blob_id` was recognised by other service instance and was unlinked."""
        if current:
            logging.info("Blob has been successfully unlinked.")
        else:
            raise Ice.UnknownException("No current object provided.")

class BlobQuery(IceDrive.BlobQuery):
    """Query receiver."""
    def downloadBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query for downloading an archive based on `blob_id`."""
        if current:
            logging.info(f"Received query to download blob: {blob_id}")
            data_transfer = DataTransfer("/tmp/downloaded_blob")
            proxy = current.adapter.addWithUUID(data_transfer)
            response.downloadBlobResponse(IceDrive.DataTransferPrx.uncheckedCast(proxy))
        else:
            raise Ice.UnknownException("No current object provided.")

    def doesBlobExist(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to check if a given `blob_id` is stored in the instance."""
        if current:
            logging.info(f"Received query to check blob existence: {blob_id}")
            response.blobExists()
        else:
            raise Ice.UnknownException("No current object provided.")

    def linkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to create a link for `blob_id` archive if it exists."""
        if current:
            logging.info(f"Received query to link blob: {blob_id}")
            response.blobLinked()
        else:
            raise Ice.UnknownException("No current object provided.")

    def unlinkBlob(self, blob_id: str, response: IceDrive.BlobQueryResponsePrx, current: Ice.Current = None) -> None:
        """Receive a query to destroy a link for `blob_id` archive if it exists."""
        if current:
            logging.info(f"Received query to unlink blob: {blob_id}")
            response.blobUnlinked()
        else:
            raise Ice.UnknownException("No current object provided.")