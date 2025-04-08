from engine.awsUploader import awsUploader
from engine.aws_upload_config import aws_config


def main():

    # Create the source and destination RDS instances
    uploader = awsUploader() # קונפיג ריק לשלב זה
    uploader.create_rds_instance(aws_config)


if __name__ == "__main__":
    main()
