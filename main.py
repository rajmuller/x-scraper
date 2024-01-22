from datetime import date
from pathlib import Path
import os
import sys
from twitter.scraper import Scraper
from twitter.util import find_key
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

email, username, password = 'javabien@protonmail.me', 'jbien1290', 'pl4ceh0ld3r'
scraper = Scraper(
    cookies={"ct0": os.environ["CT0"], "auth_token": os.environ["AUTH_TOKEN"]})

history_file_path = Path('i-o/follows.csv')
stalking_file_path = Path('i-o/stalking.csv')


#### Define functions ####

def get_user_ids(users: dict) -> pd.DataFrame:
    """
    Extracts user IDs and screen names from a dictionary of users.

    Args:
        users (dict): A dictionary containing user information.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted user IDs and screen names.
    """
    d = []
    for u in find_key(users, 'user'):
        x = u.get('result', {})
        y = x.get('rest_id')
        w = x.get('screen_name')
        if z := x.get('legacy', {}):
            d.append({'rest_id': y, 'screen_name': w} | z)

    df = (
        pd.DataFrame(d)
        .drop_duplicates('rest_id')
    )

    return df


def get_follows(follow_dat, mark) -> pd.DataFrame:
    """
    Retrieves the follows data from the given follow_dat and returns it as a pandas DataFrame.

    Parameters:
    follow_dat (list): A list of follow data.
    mark (str): The mark value.

    Returns:
    pd.DataFrame: A DataFrame containing the follows data.
    """
    d = []
    # Add your code here
    for p in follow_dat:
        # each user's follows
        for q in find_key(p, 'user_results'):
            x = q.get('result', {})
            y = x.get('rest_id')
            w = x.get('screen_name')
            if z := x.get('legacy', {}):
                d.append({'mark': mark, 'rest_id': y, 'screen_name': w} | z)

    return d


#### Start crawling ####
def main():
    """
    Main function that performs the following tasks:
    1. Reads data from a CSV file if it exists.
    2. Scrapes user data using the marks obtained from the CSV file.
    3. Retrieves user IDs from the scraped data.
    4. Retrieves the followers of each user.
    5. Merges the scraped data with the existing history data.
    6. Generates a report of new follows and saves it to a CSV file.
    7. Saves the merged data to the history file.
    """
    if stalking_file_path.is_file():
        stalking = pd.read_csv(stalking_file_path)
        marks = stalking['Mark'].tolist()
    else:
        sys.exit("stalking.csv missing")

    users = scraper.users(marks)
    rest_ids = get_user_ids(users)

    d_outer = []
    for _, row in rest_ids.iterrows():
        follows = get_follows(scraper.following([int(row['rest_id'])]),
                              row['screen_name'])
        d_outer += follows

    merged = pd.DataFrame(d_outer)
    merged['follow_order'] = merged.groupby(
        'mark').cumcount(ascending=False).add(1)

    if history_file_path.is_file():

        print("History found")

        old_history = pd.read_csv(history_file_path).astype(str)

        outer = merged.astype(str).merge(
            old_history, on=['mark', 'rest_id'], how='outer', indicator=True)
        new_follows = outer[outer['_merge'] == 'left_only']

        length = new_follows.shape[0]

        if length > 0:
            d = date.today()

            print(f"{length} new follows to report")

            filename = f"{d:%Y-%m-%d} - {length} new follows.csv"
            outpath = Path('i-o/'+filename)

            new_follows['twitter_url'] = "https://twitter.com/" + \
                new_follows['screen_name_x']
            new_follows[['mark', 'screen_name_x', 'created_at_x', 'description_x',
                        'followers_count_x', 'twitter_url']].to_csv(outpath)
        else:
            print("No new follows to report")

    else:
        print("No history found")

    merged.to_csv(history_file_path)


main()
