import os
from dotenv import load_dotenv
import json
from collections import defaultdict
import requests
import pandas as pd
import progressbar


load_dotenv()
API_KEY = os.getenv('API_KEY')


policy_marker_codelist = {
    '1': 'gender_equality',
    '2': 'environment',
    '3': 'pdgg',
    '4': 'trade',
    '5': 'bio_diversity',
    '6': 'climate_mitigation',
    '7': 'climate_adaptation',
    '8': 'desertification',
    '9': 'rmnch',
    '10': 'drr',
    '11': 'disability',
    '12': 'nutrition'
}


def all_languages(xml_lang, title_narrative_lang, description_narrative_lang):
    if xml_lang is not None:
        return list(
            set(
                [xml_lang] + title_narrative_lang + description_narrative_lang
            )
        )
    return list(
        set(
            title_narrative_lang + description_narrative_lang
        )
    )


def parse_policy_markers(policy_marker_codes, policy_marker_significances):
    results = {'{}_sig'.format(marker_name): '0' for marker_name in policy_marker_codelist.values()}
    # Sense check
    if len(policy_marker_codes) == len(policy_marker_significances):
        for marker_code, marker_name in policy_marker_codelist.items():
            if marker_code in policy_marker_codes:
                marker_index = policy_marker_codes.index(marker_code)
                marker_sig = policy_marker_significances[marker_index]
                results['{}_sig'.format(marker_name)] = marker_sig
    return results


def main():
    reporting_org_relevance = defaultdict(lambda: set())
    # Use the IATI Datastore API to fetch data
    rows = 1000
    next_cursor_mark = '*'
    current_cursor_mark = ''
    iteration = 0
    with progressbar.ProgressBar(max_value=1) as bar:
        while next_cursor_mark != current_cursor_mark:
            iteration += 1
            results = []
            url = (
                'https://api.iatistandard.org/datastore/activity/select'
                '?q=(*:*)'
                '&sort=id asc'
                '&wt=json&fl=iati_identifier,reporting_org_ref,xml_lang,title_narrative,title_xml_lang,description_narrative,description_xml_lang,policy_marker_code,policy_marker_significance&rows={}&cursorMark={}'
            ).format(rows, next_cursor_mark)
            api_json_str = requests.get(url, headers={'Ocp-Apim-Subscription-Key': API_KEY}).content
            api_content = json.loads(api_json_str)
            if bar.max_value == 1:
                bar.max_value = api_content['response']['numFound']
            activities = api_content['response']['docs']
            len_results = len(activities)
            current_cursor_mark = next_cursor_mark
            next_cursor_mark = api_content['nextCursorMark']
            for activity in activities:
                results_dict = {}
                results_dict['iati_identifier'] = activity['iati_identifier']
                org_ref = activity['reporting_org_ref']
                results_dict['reporting_org_ref'] = org_ref
                results_dict['text'] = ' '.join(activity.get('title_narrative', []) + activity.get('description_narrative', []))
                results_dict['languages'] = '|'.join(all_languages(activity.get('xml_lang'), activity.get('title_xml_lang', []), activity.get('description_xml_lang', [])))
                policy_marker_codes = activity.get('policy_marker_code', [])
                results_dict.update(parse_policy_markers(policy_marker_codes, activity.get('policy_marker_significance', [])))
                reporting_org_relevance[org_ref].update(policy_marker_codes)
                results.append(results_dict)
            df = pd.DataFrame.from_records(results)
            df.to_csv('data/{}.csv'.format(iteration), index=False)
            if bar.value + len_results <= bar.max_value:
                bar.update(bar.value + len_results)

    relevance_records = []
    for org_ref, policy_marker_codes_set in reporting_org_relevance.items():
        org_dict = {marker_name: False for marker_name in policy_marker_codelist.values()}
        org_dict['reporting_org_ref'] = org_ref
        for marker_code, marker_name in policy_marker_codelist.items():
            if marker_code in policy_marker_codes_set:
                org_dict[marker_name] = True
        relevance_records.append(org_dict)

    relevance_df = pd.DataFrame.from_records(relevance_records)
    relevance_df.to_csv('relevance.csv', index=False)

if __name__ == '__main__':
    main()