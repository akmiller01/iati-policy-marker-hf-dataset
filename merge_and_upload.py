from huggingface_hub import login
from datasets import Dataset
from dotenv import load_dotenv
import pandas as pd
import glob
from tqdm import tqdm
import os


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


def main():
    df_list = list()
    csv_filenames = glob.glob('./data/*.csv')
    for csv_filename in tqdm(csv_filenames):
        df_list.append(pd.read_csv(csv_filename))
    all_data = pd.concat(df_list, ignore_index=True)
    relevance_df = pd.read_csv('relevance.csv')
    all_data = all_data.merge(relevance_df, how="left", on="reporting_org_ref")
    for marker_name in policy_marker_codelist.values():
        all_data = all_data.loc[all_data['{}_sig'.format(marker_name)].isin([0, 1, 2, 3, 4])]
        all_data = all_data.loc[all_data[marker_name].isin([True, False])]
    dataset = Dataset.from_pandas(all_data, preserve_index=False)
    dataset.save_to_disk("dataset")
    dataset.push_to_hub("alex-miller/iati-policy-markers")


if __name__ == '__main__':
    load_dotenv()
    HF_TOKEN = os.getenv('HF_TOKEN')
    login(token=HF_TOKEN)
    main()