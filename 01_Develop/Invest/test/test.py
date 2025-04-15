import mojito
import pprint, yaml


# config.yaml 불러오기
with open("config/config.yaml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)


key = config['paper_app']
secret = config['paper_sec']
acc_no=config['my_paper_stock']+'-01'

broker = mojito.KoreaInvestment(
    api_key=key,
    api_secret=secret,
    acc_no=acc_no,
    exchange='나스닥',
    mock = True
)

balance = broker.fetch_present_balance()
pprint.pprint(balance)