import pymsteams as tm

def send_message(text):
    connector = tm.connectorcard("https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/fa76405c7bcc405a9c7e0404814b99ac/384edb55-72b2-4eb3-9600-385bcd68c8b8")
    connector.text(text)
    connector.send()

def failure_notice(scraper_name):
    connector = tm.connectorcard("https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/fa76405c7bcc405a9c7e0404814b99ac/384edb55-72b2-4eb3-9600-385bcd68c8b8")
    connector.text('The scraper failed for ' + scraper_name)
    connector.send()