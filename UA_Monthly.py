import os
import time
import datetime
import pandas as pd
import calendar
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = os.path.expanduser('~/PATH TO YOUR GOOGLE API KEY JSON FILE')
credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION)
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)


def initialize_analytics_reporting():
    """
    Initializes the Google Analytics Reporting API client.
    Establishes and returns a service object to interact with the Google Analytics API.

    :return: The Google Analytics Reporting API client.
    """
    creds = Credentials.from_service_account_file(
        KEY_FILE_LOCATION, scopes=SCOPES)

    # Build the service object
    analytics = build('analyticsreporting', 'v4', credentials=creds)

    return analytics


def fetch_ids():
    """
    Fetches records from a BigQuery table. Each record is expected to contain at least a domain name and
    a corresponding view ID.

    The function collects these entries and returns them as a list of tuples, where each tuple
    contains:
    - domain (str): The domain name associated with the view.
    - view_id (str): The identifier for the view associated with the domain.

    :return: List[Tuple[str, str]]: A list of tuples, each containing the domain and its associated view ID.
    """
    sql = f"""
    SELECT * FROM `YOUR TABLE IN BIGQUERY`
    """

    # Execute the query
    properties_job = bigquery_client.query(sql)

    # Get the results
    properties_results = properties_job.result()

    return [(row.domain, row.view_id) for row in properties_results]


def get_organic_filter():
    """
    :returns: a filter for organic traffic in universal analytics
    """
    return [{
        'filters': [{
            'dimensionName': 'ga:channelGrouping',
            'operator': 'EXACT',
            'expressions': ['Organic Search']
        }]
    }]


def get_blog_filter():
    """
    :return: Constructs a filter for blog traffic. Filter's on Landing Page dimension
    """
    return [{
        'filters': [{
            'dimensionName': 'ga:landingPagePath',
            'operator': 'REGEXP',
            'expressions': ['blog'],
            'not': True  # This inverts the filter, excluding the matched pages
        }]
    }]


def get_blog_organic_filter():
    """
    :return: Combines the previous 2 filters and returns a single filter that filters on blog traffic, and
    organic traffic returning organic non blog traffic for a particular site. Use this as a base to construct
    other filters https://developers.google.com/analytics/devguides/reporting/core/v4/basics#filtering

    """
    return [{
        'operator': 'AND',
        'filters': [{
            'dimensionName': 'ga:channelGrouping',
            'operator': 'EXACT',
            'expressions': ['Organic Search']
        },
            {
            'dimensionName': 'ga:landingPagePath',
            'operator': 'REGEXP',
            'expressions': ['blog'],
            'not': True  # This inverts the filter, excluding the matched pages
        }]
    }]


def get_report(analytics, view_id, start_date, end_date, max_retries=3):
    """
    Retrieves a basic traffic report (users and sessions) for specified dates without any filters.

    :param analytics: Initialized Google Analytics client.
    :param view_id: Analytics view ID.
    :param start_date: Start date of the report period.
    :param end_date: End date of the report period.
    :param max_retries: Maximum number of retry attempts for API requests.
    :return: Traffic report response
    """
    retries = 0
    while retries < max_retries:
        try:
            response = analytics.reports().batchGet(
                body={
                    'reportRequests': [{
                        'viewId': view_id,
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        'dimensions': [],
                        'metrics': [{'expression': 'ga:users'},
                                    {'expression': 'ga:sessions'}]
                    }]
                }
            ).execute()
            return response

        except Exception as e:
            print(f'Error fetching data for id: {view_id} on {start_date}: {e}. Attempt {retries+1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def get_organic_report(analytics, view_id, start_date, end_date, max_retries=3):
    """
    Retrieves a traffic report filtered by organic traffic for specified dates.

    :param analytics: Initialized Google Analytics client.
    :param view_id: Analytics view ID.
    :param start_date: Start date of the report period.
    :param end_date: End date of the report period.
    :param max_retries: Maximum number of retry attempts for API requests.
    :return: Traffic report including organic traffic data.
    """
    retries = 0
    while retries < max_retries:
        try:
            response = analytics.reports().batchGet(
                body={
                    'reportRequests': [{
                        'viewId': view_id,
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        'dimensions': [],
                        'metrics': [{'expression': 'ga:users'},
                                    {'expression': 'ga:sessions'}],
                        'dimensionFilterClauses': get_organic_filter()  # Using filters function to keep things organized
                    }]
                }
            ).execute()
            return response

        except Exception as e:
            print(f'Error fetching data for id: {view_id} on {start_date}: {e}. Attempt {retries+1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def get_report_filtered(analytics, view_id, start_date, end_date, max_retries=3):
    """
    Retrieves a traffic report filtering out blog traffic for specified dates.

    :param analytics: Initialized Google Analytics client.
    :param view_id: Analytics view ID.
    :param start_date: Start date of the report period.
    :param end_date: End date of the report period.
    :param max_retries: Maximum number of retry attempts for API requests.
    :return: Traffic report not including blog traffic
    """
    retries = 0
    while retries < max_retries:
        try:
            filter_expression = get_blog_filter()

            response = analytics.reports().batchGet(
                body={
                    'reportRequests': [{
                        'viewId': view_id,
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        'dimensions': [],
                        'metrics': [{'expression': 'ga:users'},
                                    {'expression': 'ga:sessions'}],
                        'dimensionFilterClauses': filter_expression
                    }]
                }
            ).execute()
            return response

        except Exception as e:
            print(f'Error fetching data for id: {view_id} on {start_date}: {e}. Attempt {retries+1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def get_organic_report_filtered(analytics, view_id, start_date, end_date, max_retries=3):
    """
    Retrieves a traffic report for specified dates, filtered to include only organic and non-blog traffic.

    :param analytics: Initialized Google Analytics client.
    :param view_id: Analytics view ID.
    :param start_date: Start date of the report period.
    :param end_date: End date of the report period.
    :param max_retries: Maximum number of retry attempts for API requests.
    :return: Traffic report filtered for organic and excluding blog traffic.
    """
    retries = 0
    while retries < max_retries:
        try:
            filter_expression = get_blog_organic_filter()

            response = analytics.reports().batchGet(
                body={
                    'reportRequests': [{
                        'viewId': view_id,
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        'dimensions': [],
                        'metrics': [{'expression': 'ga:users'},
                                    {'expression': 'ga:sessions'}],
                        'dimensionFilterClauses': filter_expression
                    }]
                }
            ).execute()
            return response

        except Exception as e:
            print(f'Error fetching data for id: {view_id} on {start_date}: {e}. Attempt {retries+1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def write_to_df(response):
    """
    Converts Google Analytics API response data into a pandas DataFrame.

    :param response: The Google Analytics API response to convert.
    :return: A DataFrame containing the API response data, structured for analysis.
    """
    # Check if response is None or does not contain 'reports
    if response is None or 'reports' not in response:
        print("invalid or None response received.")
        return pd.DataFrame()

    all_rows = []
    columns = []

    for report in response.get('reports', []):
        column_header = report.get('columnHeader', {})
        dimension_headers = column_header.get('dimensions', [])

        # Correctly extract metric header names
        metric_headers = [metricHeaderEntry.get('name') for metricHeaderEntry in
                          column_header.get('metricHeader', {}).get('metricHeaderEntries', [])]

        # Combine dimension and metric headers for DataFrame columns
        columns = dimension_headers + metric_headers

        rows = report.get('data', {}).get('rows', [])

        for row in rows:
            dimensions = row.get('dimensions', [])
            metrics = [value for metrics in row.get('metrics', []) for value in metrics.get('values', [])]

            row_data = dimensions + metrics
            all_rows.append(row_data)

    # Ensure columns are correctly set as strings
    columns = [str(col) for col in columns]

    df = pd.DataFrame(all_rows, columns=columns)
    return df


def main():
    # Timing how long the script takes to run
    start_time = time.time()

    # Initializing the analytics client
    analytics = initialize_analytics_reporting()

    # Initializing dataframe to append/concat date to
    accumulated_df = pd.DataFrame()

    # Start Date you want for you pull
    start_date = datetime.date(2019, 1, 1)
    # Note this is the end date range of the entire pull this is different from the end date within each function
    end_date = datetime.date(2023, 6, 30)
    curr_date = start_date

    while curr_date <= end_date:

        # Grabs the last day of the month for the end date range parameter within the reporting functions
        last_day = calendar.monthrange(curr_date.year, curr_date.month)[1]
        month_end_date = datetime.date(curr_date.year, curr_date.month, last_day)

        for domain, view_id in fetch_ids():

            print(f'Running Current Date: {curr_date} for {domain}')

            start_date_str = curr_date.strftime('%Y-%m-%d')
            end_date_str = month_end_date.strftime('%Y-%m-%d')

            organic_response = get_organic_report(analytics, view_id, start_date_str, end_date_str)
            total_response = get_report(analytics, view_id, start_date_str, end_date_str)
            organic_response_filter = get_organic_report_filtered(analytics, view_id, start_date_str, end_date_str)
            total_response_filter = get_report_filtered(analytics, view_id, start_date_str, end_date_str)

            if organic_response is not None and total_response is not None:
                df_organic = write_to_df(organic_response)
                df_total = write_to_df(total_response)
                df_organic_filtered = write_to_df(organic_response_filter)
                df_total_filtered = write_to_df(total_response_filter)

                df_organic.rename(columns={"ga:users": "organicTotalUsers",
                                           "ga:sessions": "organicSessions"},
                                  inplace=True)
                df_total.rename(columns={"ga:users": "totalUsers",
                                         "ga:sessions": "totalSessions"},
                                inplace=True)
                df_organic_filtered.rename(columns={"ga:users": "organicTotalUsersFiltered",
                                                    "ga:sessions": "organicSessionsFiltered"},
                                           inplace=True)
                df_total_filtered.rename(columns={"ga:users": "totalUsersFiltered",
                                                  "ga:sessions": "totalSessionsFiltered"},
                                         inplace=True)

                df_organic['month'] = start_date_str
                df_total['month'] = start_date_str
                df_organic_filtered['month'] = start_date_str
                df_total_filtered['month'] = start_date_str

                # Merging the data frames
                df_merged = pd.merge(df_total, df_organic, on=["month"], how="left")
                df_merged_filter = pd.merge(df_total_filtered, df_organic_filtered, on=["month"], how="left")
                df_combined = pd.merge(df_merged, df_merged_filter, on=["month"], how="left")

                df_combined.fillna(0, inplace=True)

                df_combined['domain'] = domain

                accumulated_df = pd.concat([accumulated_df, df_combined], ignore_index=True)

        curr_date += relativedelta(months=1)

    file_path_str = f"~/Downloads/{datetime.date.today} UA Monthly.csv"
    file_path = os.path.expanduser(file_path_str)
    accumulated_df.to_csv(file_path, index=False)

    end_time = time.time()
    print(f'Process took {round((end_time - start_time), 2)}')


if __name__ == '__main__':
    main()
