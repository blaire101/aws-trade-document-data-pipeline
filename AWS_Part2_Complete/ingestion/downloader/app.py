"""Fargate SFTP batch downloader for client-provided CSV files.

SFTP is used here as a secure-file-transfer architecture assumption for the portfolio case study.
"""
import io, json, logging, os
from pathlib import PurePosixPath
import boto3, paramiko

LOG=logging.getLogger("trade-sftp-downloader"); logging.basicConfig(level=logging.INFO)

def main():
    bucket=os.environ["TARGET_BUCKET"]; prefix=os.getenv("STAGED_PREFIX","raw/staged")
    secret_id=os.environ["SFTP_SECRET_ID"]; host=os.environ["SFTP_HOST"]; port=int(os.getenv("SFTP_PORT","22")); remote_dir=os.getenv("SFTP_REMOTE_DIR","/outbound/cinv")
    secret=json.loads(boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"])
    key=paramiko.RSAKey.from_private_key(io.StringIO(secret["private_key"]))
    transport=paramiko.Transport((host,port)); transport.connect(username=secret["username"],pkey=key)
    sftp=paramiko.SFTPClient.from_transport(transport); s3=boto3.client("s3")
    try:
        for name in sorted(sftp.listdir(remote_dir)):
            if not name.lower().endswith(".csv"): continue
            safe=PurePosixPath(name).name
            with sftp.open(f"{remote_dir.rstrip('/')}/{safe}","rb") as source:
                s3.upload_fileobj(source,bucket,f"{prefix.rstrip('/')}/{safe}",ExtraArgs={"ContentType":"text/csv"})
            LOG.info("Staged %s to s3://%s/%s/%s",safe,bucket,prefix,safe)
    finally:
        sftp.close(); transport.close()
if __name__=="__main__": main()
