import datetime
import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

sheet_id = "1aCXY7VUcHlBDjtNjrD2io528KkE6uf3QjfLNsHiQxsc"
api_json = {
  "type": "service_account",
  "project_id": "scraping-web-traffic-for-alex",
  "private_key_id": "253e29a997c206c4b2aa98262b21b67fe08ebbb7",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDEoYp5UVAMEJfi\ngo6E4Eflk3ZlP9rfoFkYfwBz3NnYV+b2btvK47MfFWT5DQGTliqiKss2encHFrwX\nTPnxpyIyGezeFVvpJpGOwC6pdStd2BrsL8oz3o9X6kqN/nwkIVq4rhzprwtnLRch\nka9PNrLBYE+iYk0KeBmJChoWJdJ5YfAS8b/nR3n7as4J+NlNa+9dt2HUGgqPwGhI\nfIu7sYtWBk2KxvNdwOMFDpxd46nYZhWD0kBriqgV9jaXwS2S3tUNwBzvR7atAyox\nEKn9M/VAK68pxJuTESK/r6i9M5eOu1dVbixhVsDlw/jxvBJlhAmyz1m6tmNxKwGv\nKW6FjhtJAgMBAAECggEADjtIbdkxJ1xkl9n99XdTsNW0WCXv4idlh3C6wEgL8t0C\n3BOAQD4sgtuOLjnvIybXYVsiT+g7ePV/6dpSDVfc0V4/vv+yAi0e1COMBQsUV1Oa\nuD1Jv+kT/Ceam44fn1BUNAFzGYVIOKCid9Um6H9znuwo+15sx80TQRBMfM2iPxNz\nfFb6dTYbsgY/7f+PkWadNkimH7EIs+CcbpoT1aY11DIZB5kg/Y0RIxQFeU4bAAjQ\n2GLI8EHwZ6Qe4jjA+r+RRGPJpCqvWHpYin+l9zNcDJkGQB3Y7uQpcz/JahCaDPju\nvrzpeQTOvXZNC9bOXow3T53uJ7avrQXscdsC18fPgQKBgQDk0kXexKO9rnlVb7hm\nHd6sTOF5uD4ZfaUw6GKP3emcX9Wpfn0JKvsCul2RXOQKyv15DsScD6TfjZLqD28y\neycfmriDPU3zjkTrJkMDbw+X2C3sE/bDmZc+gZd4JdMFNZKm6j0mozTwZl7A+pFC\nLiZ5Ug6xYxzRPriME+tag4e6gQKBgQDb/HZFajNEXz32a5TqiuElQLpcJyhH/oS8\n62CZrQe4bhhXlsKuc1hQSQKMYtk+4j4YlYlFEpe6KK8nCCd+3feYTs5dDj1wnj5j\n6YAXRCpUHLgn2mJyiCCSsSHcoq7egmt/v7F8lqGTELGlfXWXTlhpJN3XNtCepWXa\nNjFZy66syQKBgH5zeWs/LeGv3puJTtUX4qtDHUN/vsmldrmNYpJwpx3klBXDseN0\nHh0G0ph3Rgp03RieQaagupNUEIFKoD+8cQV1Ikxcf4x97bYpgdUO9gYK0G3rJm43\nna5MPWGHPZNeZLnbLooAnUvQtsJcdhOln1tiLn528EDuMVwv0dtGXqaBAoGAX82B\nwFcwvnzv4ioV71LOHEglIM7Yxwm7yv6C0ko9i38+0J5SaGDJPCWfm33M+IrU2iX9\nYvxm4WaoaMovURvfoQ/o5TO3ZB02NJLS+s7v1DOxq4F60EAiV2AWzBx5JWQM5tSX\nuhdrhD2H7/dER8GR1TB+ACTaE80aHz7snRuMFakCgYAz3e3MoZ6UdJbCmwWVbU6v\nmSlZWqvieP4iouUMCeZASZr9JNAfqDMU1OFXGTcUjK68Ng/I5NTtmeGsOIQU05Ez\nBWgPtaTsoauZu+FDhJ0eKnd32+I5F6xzMB+z50Z/WoGBiqQrB8orR6r9wG8YWPtq\n7g6xT3+PhzuX5BBvLk9iRQ==\n-----END PRIVATE KEY-----\n",
  "client_email": "google-sheets-integration@scraping-web-traffic-for-alex.iam.gserviceaccount.com",
  "client_id": "101182097852623601456",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/google-sheets-integration%40scraping-web-traffic-for-alex.iam.gserviceaccount.com"
}


def get_service_sacc():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_service = ServiceAccountCredentials.from_json_keyfile_dict(api_json, scopes=scopes).authorize(httplib2.Http())
    return build(serviceName="sheets", version="v4", http=creds_service)


def get_data_1(page):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    response = sheet.values().get(spreadsheetId=sheet_id, range=f"{page}!A2:A100000").execute()

    result = list()
    for element in response["values"]:
        if element[0] not in result:
            result.append(element[0].strip())
    return result


def get_data_pr():
    service = get_service_sacc()
    sheet = service.spreadsheets()
    response = sheet.values().get(spreadsheetId=sheet_id, range=f"PR!B2:B100000").execute()

    result = list()
    for element in response["values"]:
        if element[0] not in result:
            result.append(element[0].strip())
    return result


def record_data_pr(total_visits, domain, index):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    values = [[domain, total_visits]]
    body = {"values": values}
    sheet.values().update(spreadsheetId=sheet_id, range=f"PR!B{index}",
                          valueInputOption="RAW", body=body).execute()


def record_data_marketing_website(total_visits, source, domain, index):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    values = [[domain, source, total_visits]]
    body = {"values": values}
    sheet.values().update(spreadsheetId=sheet_id, range=f"Marketing/website!A{index}",
                          valueInputOption="RAW", body=body).execute()
    
    
def record_data_funds(domain, total_visits, index):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    values = [[domain, total_visits]]
    body = {"values": values}
    sheet.values().update(spreadsheetId=sheet_id, range=f"Funds!A{index}",
                          valueInputOption="RAW", body=body).execute()


def record_data_launchpads(domain, total_visits, index):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    values = [[domain, total_visits]]
    body = {"values": values}
    sheet.values().update(spreadsheetId=sheet_id, range=f"Launchpads!A{index}",
                          valueInputOption="RAW", body=body).execute()


def record_data_pr500(domain, total_visits, index):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    values = [[domain, total_visits]]
    body = {"values": values}
    sheet.values().update(spreadsheetId=sheet_id, range=f"pr 500+ media!A{index}",
                          valueInputOption="RAW", body=body).execute()


def get_data_2():
    service = get_service_sacc()
    sheet = service.spreadsheets()
    response = sheet.values().get(spreadsheetId=sheet_id, range=f"Capitaliztion!C2:C100000").execute()

    result = list()
    for element in response["values"]:
        if element[0] not in result:
            result.append(element[0].strip())
    return result


def record_data_2(market_cap, index):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    values = [[market_cap]]
    body = {"values": values}
    sheet.values().update(spreadsheetId=sheet_id, range=f"Capitaliztion!D{index}",
                          valueInputOption="RAW", body=body).execute()


def update_status(status, index=None, page=None, amount_domains=None):
    service = get_service_sacc()
    sheet = service.spreadsheets()
    if status == "В работе":
        left = amount_domains - index
        percent = int((index / amount_domains) * 100)
        values = [[status, page, index, left, f"{percent}%"]]
    else:
        values = [[status, "-", "-", "-", "-"]]
    body = {"values": values}
    sheet.values().update(spreadsheetId=sheet_id, range=f"Парсер!B2",
                          valueInputOption="RAW", body=body).execute()


if __name__ == "__main__":
    print("[INFO] Чтобы запустить парсер, запустите скрипт main.py")
