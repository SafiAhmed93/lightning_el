import os
import sys
import argparse
from azure.communication.email import EmailClient

def main():
    parser = argparse.ArgumentParser(description="Send an email via Azure Communication Services")
    parser.add_argument("--to", required=True, help="Recipient email addresses (comma-separated)")
    parser.add_argument("--cc", required=False, help="CC email addresses (comma-separated)")
    parser.add_argument("--subject", required=True, help="Email subject")
    parser.add_argument("--content", required=True, help="Email content (plain text)")
    args = parser.parse_args()

    try:
        connection_string = os.getenv("AZURE_COMMUNICATION_CONNECTION_STRING")
        if not connection_string:
            print("Error: AZURE_COMMUNICATION_CONNECTION_STRING environment variable is not set.", file=sys.stderr)
            sys.exit(1)
            
        client = EmailClient.from_connection_string(connection_string)

        to_addresses = [email.strip() for email in args.to.split(",") if email.strip()]
        recipients_payload = {
            "to": [{"address": email} for email in to_addresses]
        }
        
        if args.cc:
            cc_addresses = [email.strip() for email in args.cc.split(",") if email.strip()]
            if cc_addresses:
                recipients_payload["cc"] = [{"address": email} for email in cc_addresses]

        message = {
            "senderAddress": "DoNotReply@eead9f27-274e-4751-846c-1e7912670528.azurecomm.net",
            "recipients": recipients_payload,
            "content": {
                "subject": args.subject,
                "plainText": args.content,
            },
        }

        poller = client.begin_send(message)
        result = poller.result()
        print(f"Message sent: {result}")

    except Exception as ex:
        print(f"Error: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()