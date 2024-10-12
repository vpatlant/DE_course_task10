import requests

class Tg:
    def __init__(self, tg_token, telegram_channel_id, disable_web_page_preview = 'true'):
        self.tg_token = tg_token
        self.telegram_channel_id = telegram_channel_id
        self.disable_web_page_preview = disable_web_page_preview
        
    def send_text(self, text):
        method = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"

        try:
            r = requests.post(method, data={"chat_id": self.telegram_channel_id, "text": text,
                                            "disable_web_page_preview": self.disable_web_page_preview})
            
            if r.status_code != 200:
                print('Error', r.status_code)
                print('Error', r.text)

        except Exception as e:
            print(str(e))