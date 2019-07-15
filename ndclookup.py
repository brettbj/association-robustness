import sys
import requests
import argparse
import pandas as pd
import xml.etree.ElementTree as ET

from multiprocessing import Pool
from tqdm import tqdm
from requests.exceptions import ConnectionError
from proxy_requests import ProxyRequests

BASE_URL = "https://rxnav.nlm.nih.gov/REST"
ALL_NDCS_FP = 'all_drugs.csv'

err_sink = sys.stdout
proxies = ''

def rotate_proxy(test_url=BASE_URL+'/version'):
    rotator = ProxyRequests(test_url)
    rotator.get()
    proxy = rotator.get_proxy_used
    proxies = {'http': 'http://%s' % proxy, 'https': 'https://%s' % proxy}


def get_with_proxy(url):
    return requests.get(url, proxies=proxies)


def get_rxcui(ndc, base_url=BASE_URL):
    if len(ndc) < 11:
        ndc = fix_ndc(ndc)
    resp = get_with_proxy(base_url + '/ndcstatus?ndc=' + ndc)
    root = ET.fromstring(resp.text)

    if not root.find('./ndcStatus/rxcui').text:
        print('NDC %s did not map to RxCUI' % ndc, file=err_sink)
        return None
    else:
        return root.find('./ndcStatus/rxcui').text


def get_ingredients(rxcui, base_url=BASE_URL):
    if not rxcui:
        return []

    resp = get_with_proxy(base_url + '/rxcui/' + rxcui + '/allrelated')
    root = ET.fromstring(resp.text)

    ingredients = root.findall("./allRelatedGroup/conceptGroup/[tty='IN']/conceptProperties/rxcui")
    if not ingredients:
        print('No ingredients found for RxCUI %s' % rxcui, file=err_sink)

    return [n.text for n in ingredients]


def ndc_to_ingredients(ndc, base_url=BASE_URL):
    try:
        fixed_ndc = fix_ndc(ndc)
        rxcui = get_rxcui(fixed_ndc, base_url)
        ingredients = get_ingredients(rxcui, base_url)
        return (ndc, rxcui, ingredients)
    except ConnectionError:
        rotate_proxy()
        ndc_to_ingredients(ndc)


def fix_ndc(ndc):
    if len(ndc) < 8:
        return '0000' + ndc
    elif len(ndc) == 8:
        return '000' + ndc
    elif len(ndc) == 9:
        return '00' + ndc
    elif len(ndc) == 10:
        return '0' + ndc
    else:
        return ndc


def unfold_results(results):
    rows_list = []
    for ndc, rxcui, ingredients in results:
        for ingredient in ingredients:
            rows_list.append({'ndc': ndc, 'rxcui': rxcui, 'ingredient_rxcui': ingredient})
        if not ingredients:
            rows_list.append({'ndc': ndc, 'rxcui': rxcui, 'ingredient_rxcui': None})

    return pd.DataFrame(rows_list)


def mp_ingredients_lookup(all_drugs, n_threads):
    with Pool(args.n_threads) as pool:
       results = list(tqdm(pool.imap(ndc_to_ingredients, all_drugs.index), total=len(all_drugs)))
    return results


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('n_threads', type=int)
    parser.add_argument('--base_url', default=BASE_URL)
    parser.add_argument('--all_drugs_file', default=ALL_NDCS_FP)
    parser.add_argument('--error_file', default=None)
    args = parser.parse_args()

    if args.error_file:
        err_sink = args.error_file
    all_drugs = pd.read_csv(args.all_drugs_file, dtype={'NationalDrugCode':str})
    all_drugs.set_index('NationalDrugCode', inplace=True)

    results = mp_ingredients_lookup(all_drugs, args.n_threads)

    df = unfold_results(results)
    df.to_csv('drug_ingredients.csv', columns=['ndc', 'rxcui', 'ingredient_rxcui'])
