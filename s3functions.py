from io import StringIO, BytesIO
import pandas as pd
import json
import os


def upload_to_s3(csv_df, file_key, s3_client, bucket_name):

    csv_buffer = StringIO()
    csv_df.to_csv(csv_buffer, index=False)

    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        status = f"DataFrame uploaded to {bucket_name}/{file_key} successfully."
    except Exception as e:
        status = "Error uploading DataFrame to S3: {e}"

    return status


def get_data_from_s3(client, file_key, bucket_name):

    response = client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()

    df = pd.read_csv(BytesIO(file_content))

    return df


def upload_json(json_object, client, file_key, bucket_name):

    json_data = json.dumps(json_object)
    client.put_object(Bucket=bucket_name, Key=file_key, Body=json_data)

    print('json object uploaded')


def download_json(client, file_key, bucket_name):

    response = client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')

    data = json.loads(file_content)

    return data


def upload_folder_to_s3(s3_client, folder_path, bucket_name):
    # Walk through each file and directory in the folder
    for root, dirs, files in os.walk(folder_path):
        for file in files:

            # Create the full file path
            local_path = os.path.join(root, file)

            # Create the relative path to maintain the folder structure in S3
            relative_path = os.path.relpath(local_path, folder_path)
            s3_key = 'mapping_file/' + relative_path.replace("\\", "/")  # Replace backslashes on Windows

            # Upload the file to S3
            s3_client.upload_file(local_path, bucket_name, s3_key)
            print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")





