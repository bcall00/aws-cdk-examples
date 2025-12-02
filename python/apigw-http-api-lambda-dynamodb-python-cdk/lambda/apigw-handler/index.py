# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from datetime import datetime
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Automatically instrument all AWS SDK calls
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    # Log security-relevant context
    request_context = event.get("requestContext", {})
    logger.info(json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "request_id": context.request_id,
        "source_ip": request_context.get("identity", {}).get("sourceIp"),
        "user_agent": request_context.get("identity", {}).get("userAgent"),
        "http_method": request_context.get("httpMethod"),
        "resource_path": request_context.get("resourcePath"),
    }))
    
    table = os.environ.get("TABLE_NAME")
    logging.info(f"## Loaded table name from environemt variable DDB_TABLE: {table}")
    
    if event.get("body"):
        item = json.loads(event["body"])
        # Sanitize sensitive fields before logging
        sanitized_item = {k: "***" if k in ["password", "ssn", "credit_card"] else v for k, v in item.items()}
        logging.info(f"## Received payload: {sanitized_item}")
        
        year = str(item["year"])
        title = str(item["title"])
        id = str(item["id"])
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        message = "Successfully inserted data!"
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        logging.info("## Received request without a payload")
        dynamodb_client.put_item(
            TableName=table,
            Item={
                "year": {"N": "2012"},
                "title": {"S": "The Amazing Spider-Man 2"},
                "id": {"S": str(uuid.uuid4())},
            },
        )
        message = "Successfully inserted data!"
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
