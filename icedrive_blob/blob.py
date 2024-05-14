"""Module for servants implementations."""

import Ice
import IceDrive
import logging
import os
import hashlib

class DataTransfer(IceDrive.DataTransfer):
    """Implementation of an IceDrive.DataTransfer interface."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file = open(file_path, "rb")

    def read(self, size: int, current: Ice.Current = None) -> bytes:
        """Returns a list of bytes from the opened file."""
        return self.file.read(size)

    def close(self, current: Ice.Current = None) -> None:
        """Close the currently opened file."""
        self.file.close()
        logging.info(f"File {self.file_path} closed successfully.")


class BlobService(IceDrive.BlobService):
    """Implementation of an IceDrive.BlobService interface."""

    def __init__(self):
        self.blobs = {}

    def link(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id file as linked in some directory."""
        if blob_id in self.blobs:
            self.blobs[blob_id]["links"] += 1
            logging.info(f"Blob {blob_id} linked successfully.")
        else:
            raise IceDrive.UnknownBlob(blob_id)

    def unlink(self, blob_id: str, current: Ice.Current = None) -> None:
        """Mark a blob_id as unlinked (removed) from some directory."""
        if blob_id in self.blobs:
            self.blobs[blob_id]["links"] -= 1
            if self.blobs[blob_id]["links"] == 0:
                os.remove(self.blobs[blob_id]["file_path"])
                del self.blobs[blob_id]
            logging.info(f"Blob {blob_id} unlinked successfully.")
        else:
            raise IceDrive.UnknownBlob(blob_id)

    def upload(
        self, user: IceDrive.UserPrx, blob: IceDrive.DataTransferPrx, current: Ice.Current = None
    ) -> str:
        """Register a DataTransfer object to upload a file to the service."""
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
        
        blob_id = self.calculateBlobId(file_path)
        self.blobs[blob_id] = {"file_path": file_path, "links": 1}
        
        logging.info(f"File uploaded successfully. BlobId: {blob_id}")
        return blob_id

    def download(
        self, user: IceDrive.UserPrx, blob_id: str, current: Ice.Current = None
    ) -> IceDrive.DataTransferPrx:
        """Return a DataTransfer objet to enable the client to download the given blob_id."""
        if blob_id in self.blobs:
            file_path = self.blobs[blob_id]["file_path"]
            data_transfer = DataTransfer(file_path)
            proxy = current.adapter.addWithUUID(data_transfer)
            logging.info(f"File {blob_id} downloaded successfully.")
            return IceDrive.DataTransferPrx.uncheckedCast(proxy)
        else:
            raise IceDrive.UnknownBlob(blob_id)

    def calculateBlobId(self, file_path: str) -> str:
        """Calculate the BlobId based on the file content."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as file:
            while True:
                data = file.read(4096)
                if not data:
                    break
                sha256.update(data)
        
        return sha256.hexdigest()