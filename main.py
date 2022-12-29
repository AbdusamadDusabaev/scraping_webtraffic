import datetime
import requests
from bs4 import BeautifulSoup
import time
from connect import get_data_1, get_data_2, record_data_pr, record_data_marketing_website, record_data_launchpads
from connect import record_data_funds, record_data_pr500, record_data_2, update_status, get_data_pr
from ssl import SSLEOFError, SSLZeroReturnError, SSLError
from http.client import RemoteDisconnected


timeout = 30
ua_chrome = " ".join(["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "AppleWebKit/537.36 (KHTML, like Gecko)",
                      "Chrome/108.0.0.0 Safari/537.36"])
headers = {"user-agent": ua_chrome}
proxies = {"https": f"http://services2821:f9960f@103.214.110.62:10185"}
error_count = 0


def update_ip():
    ip_update_url = "https://astroproxy.com/api/v1/ports/3359465/newip?token=65ceb7f8ae24afc3&id=3359465"
    requests.get(url=ip_update_url, headers=headers)
    print("[INFO] IP-адрес обновлен")


def get_web_traffic(domain):
    global error_count
    url = f"https://spymetrics.ru/ru/website/{domain}"
    try:
        response = requests.get(url=url, headers=headers, proxies=proxies, verify=False)
        print(f"[INFO] Status Code: {response.status_code}")
        bs_object = BeautifulSoup(response.content, "lxml")
        total_visits = bs_object.find(name="table", id="engagementTable")
        if response.status_code == 403 or total_visits is None:
            time.sleep(35)
            update_ip()
            response = requests.get(url=url, headers=headers, proxies=proxies, verify=False)
            print(f"[INFO] Status Code: {response.status_code}")
            bs_object = BeautifulSoup(response.content, "lxml")
            total_visits = bs_object.find(name="table", id="engagementTable")
            if response.status_code == 403 or total_visits is None:
                time.sleep(35)
                update_ip()
                response = requests.get(url=url, headers=headers, proxies=proxies, verify=False)
                print(f"[INFO] Status Code: {response.status_code}")
                bs_object = BeautifulSoup(response.content, "lxml")
                total_visits = bs_object.find(name="table", id="engagementTable")
                if response.status_code == 403 or total_visits is None:
                    time.sleep(35)
                    update_ip()
                    response = requests.get(url=url, headers=headers, proxies=proxies, verify=False)
                    print(f"[INFO] Status Code: {response.status_code}")
                    bs_object = BeautifulSoup(response.content, "lxml")
                    total_visits = bs_object.find(name="table", id="engagementTable")
                    if total_visits is None:
                        total_visits = "no data"
                        return {"total_visits": total_visits, "source": url}
                    else:
                        total_visits = total_visits.find(name="td", class_="text-right").text.strip()
                        return {"total_visits": total_visits, "source": url}
                else:
                    total_visits = total_visits.find(name="td", class_="text-right").text.strip()
                    return {"total_visits": total_visits, "source": url}
            else:
                total_visits = total_visits.find(name="td", class_="text-right").text.strip()
                return {"total_visits": total_visits, "source": url}
        else:
            total_visits = total_visits.find(name="td", class_="text-right").text.strip()
            return {"total_visits": total_visits, "source": url}
    except (SSLEOFError, SSLZeroReturnError, SSLError, RemoteDisconnected):
        print("[ERROR] Server's error")
        print("[INFO] Try again...")
        error_count += 1
        if error_count < 5:
            return get_web_traffic(domain)
        else:
            error_count = 0
            total_visits = "no data"
            return {"total_visits": total_visits, "source": url}


def get_market_cap(url):
    try:
        response = requests.get(url=url, headers=headers)
        bs_object = BeautifulSoup(response.content, "lxml")
        market_cap = bs_object.find(name="div", class_="statsItemRight").find(name="div", class_="statsValue").text
        return market_cap
    except AttributeError:
        return "no data"


def run_market_cap():
    domains = get_data_2()
    index = 1
    for domain in domains:
        start_time = time.time()
        print(f'[INFO] Парсим домен {domain}')
        index += 1
        if domain != "n/a":
            market_cap = get_market_cap(url=domain)
            print(f"{domain}: {market_cap}")
            record_data_2(market_cap=market_cap, index=index)
        update_status(status="В работе", index=index - 1, page="Capitaliztion", amount_domains=len(domains))
        print(f'[INFO] Парсинг домена {domain} закончен')
        stop_time = time.time()
        print(f"[INFO] Парсинг домена длился {stop_time - start_time} секунд")


def run_page_pr():
    domains = get_data_pr()
    index = 1
    for domain in domains:
        start_time = time.time()
        if index % 30 == 0:
            update_ip()
        print(f'[INFO] Парсим домен {domain}')
        index += 1
        result = get_web_traffic(domain.replace("https://", "").replace("http://", ""))
        total_visits = result["total_visits"]
        record_data_pr(total_visits=total_visits, domain=domain, index=index)
        update_status(status="В работе", index=index - 1, page="PR", amount_domains=len(domains))
        print(f'[INFO] Парсинг домена {domain} закончен')
        stop_time = time.time()
        print(f"[INFO] Парсинг домена длился {stop_time - start_time} секунд")


def run_page_marketing_website():
    domains = get_data_1(page="Marketing/website")
    index = 1
    for domain in domains:
        start_time = time.time()
        if index % 30 == 0:
            update_ip()
        print(f'[INFO] Парсим домен {domain}')
        index += 1
        result = get_web_traffic(domain.replace("https://", "").replace("http://", "").replace("/", ""))
        total_visits = result["total_visits"]
        source = result["source"]
        record_data_marketing_website(total_visits=total_visits, source=source, domain=domain, index=index)
        update_status(status="В работе", index=index - 1, page="Marketing/website", amount_domains=len(domains))
        print(f'[INFO] Парсинг домена {domain} закончен')
        stop_time = time.time()
        print(f"[INFO] Парсинг домена длился {stop_time - start_time} секунд")


def run_page_funds():
    domains = get_data_1(page="Funds")
    index = 1
    for domain in domains:
        start_time = time.time()
        if index % 30 == 0:
            update_ip()
        print(f'[INFO] Парсим домен {domain}')
        index += 1
        result = get_web_traffic(domain)
        total_visits = result["total_visits"]
        record_data_funds(total_visits=total_visits, domain=domain, index=index)
        update_status(status="В работе", index=index - 1, page="Funds", amount_domains=len(domains))
        print(f'[INFO] Парсинг домена {domain} закончен')
        stop_time = time.time()
        print(f"[INFO] Парсинг домена длился {stop_time - start_time} секунд")


def run_page_launchpads():
    domains = get_data_1(page="Launchpads")
    index = 1
    for domain in domains:
        start_time = time.time()
        if index % 30 == 0:
            update_ip()
        print(f'[INFO] Парсим домен {domain}')
        index += 1
        result = get_web_traffic(domain)
        total_visits = result["total_visits"]
        record_data_launchpads(total_visits=total_visits, domain=domain, index=index)
        update_status(status="В работе", index=index-1, page="Launchpads", amount_domains=len(domains))
        print(f'[INFO] Парсинг домена {domain} закончен')
        stop_time = time.time()
        print(f"[INFO] Парсинг домена длился {stop_time - start_time} секунд")


def run_page_pr500():
    domains = get_data_1(page="pr 500+ media")
    index = 1
    for domain in domains:
        start_time = time.time()
        if index % 30 == 0:
            update_ip()
        print(f'[INFO] Парсим домен {domain}')
        index += 1
        result = get_web_traffic(domain.replace("https://", "").replace("http://", "").replace("/", ""))
        total_visits = result["total_visits"]
        record_data_pr500(total_visits=total_visits, domain=domain, index=index)
        update_status(status="В работе", index=index-1, page="pr 500+ media", amount_domains=len(domains))
        print(f'[INFO] Парсинг домена {domain} закончен')
        stop_time = time.time()
        print(f"[INFO] Парсинг домена длился {stop_time - start_time} секунд")


def run_parser():
    update_ip()
    run_page_pr()
    run_page_marketing_website()
    run_page_funds()
    run_page_pr500()
    run_page_launchpads()
    run_market_cap()
    update_status(status="Сон")


def main():
    mode = input("[INPUT] Выберите режим работы (1 - штатный режим, 2 - проверка парсера): >>> ")
    if mode == "1":
        print("[INFO] Программа запущена")
        while True:
            today = str(datetime.date.today()).split("-")[-1]
            if int(today) == 1:
                print("[INFO] Сегодня первое число месяца. Парсер запущен")
                run_parser()
            else:
                print("[INFO] Сегодня не первое число месяца. Следующая проверка будет через 12 часов")
                time.sleep(43200)
    else:
        print("[INFO] Парсер запущен в режиме проверки")
        run_parser()
    print("[INFO] Программа успешно завершена")


if __name__ == "__main__":
    main()
