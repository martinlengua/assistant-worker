# from twilio.rest import Client

# account_sid = 'AC37522f855bedd37c5c96bc2e2d4cf5fb'
# auth_token = 'f1c431bfb8b4ff6b2cecbec87c7e4424'
# client = Client(account_sid, auth_token)

# message = client.messages.create(
#   from_='whatsapp:+14155238886',
#   content_sid='HXb5b62575e6e4ff6129ad7c8efe1f983e',
#   content_variables='{"1":"12/1","2":"3pm"}',
#   to='whatsapp:+51946166093'
# )

# print(message.sid)

import requests

def test_webhook():
    # URL for the locally hosted webhook (adjust port if necessary)
    url = "https://app-assistant-wsp.azurewebsites.net/webhook"

    # Mock data simulating a Twilio WhatsApp message
    data = {
        "Body": "help me to create a az cli command to list all subscriptions!",
        "From": "whatsapp:+1234567890"  # Mock sender number
        "To=whatsapp:+51946166093"
        "ContentSid=HXb5b62575e6e4ff6129ad7c8efe1f983e"
        'ContentVariables={"1":"12/1","2":"3pm"}'
    }

    try:
        # Send POST request to the webhook
        response = requests.post(url, data=data)
        
        # Print response from the webhook
        print("Status Code:", response.status_code)
        print("Response Text:", response.text)

    except requests.exceptions.RequestException as e:
        print("Error sending request:", e)

if __name__ == "__main__":
    test_webhook()