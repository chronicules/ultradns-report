#!/usr/bin/env python3
import sys
import access
import getopt
import requests
import json
import time
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import folium
import os
import branca.colormap as cl
import six
import numpy as np
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders


account_name = "YourUltraDNSaccountName"
smtp_server = "YourSMTPserver"
sender_email = "SenderEmail"


def get_token():
    """
    Function authorizes user and gets access token to the API
    :return: token
    """

    cred_file = 'cred.ini'

    # Read user credentials from 'creds.ini' file
    if os.path.exists(cred_file):
        with open(cred_file, 'r') as f:
            credlist = f.readlines()
            _username = access.decode(credlist[0])
            _password = access.decode(credlist[1])
    else:
        print("Run 'access.py' to create file with credentials first.")
        sys.exit(1)

    username = _username.decode("utf-8")
    password = _password.decode("utf-8")

    # Send POST request to API with username and password
    ultradnsurl = "https://api.ultradns.com/authorization/token"
    body = "grant_type=password&username=" + username + "&password=" + password
    header = {'Content-type': 'application/x-www-form-urlencoded'}

    response = requests.post(ultradnsurl, body, header)

    # Receive Bearer token from response
    token = response.json()['access_token']

    return token


def get_month():
    """
    Find start and end dates of last month:
    :return: last month start and end dates
    """
    today = datetime.date.today()

    # Get last and first days of previous month
    first = today.replace(day=1)
    lastmonth_end_date = (first - datetime.timedelta(days=1))
    lastmonth_start_date = lastmonth_end_date.replace(day=1)

    lastmonth_start = lastmonth_start_date.strftime("%Y-%m-%d")
    lastmonth_end = lastmonth_end_date.strftime("%Y-%m-%d")

    # Get lastmonth in "BBB-YYYY" format
    lastmonth = lastmonth_start_date.strftime("%b-%Y")

    return lastmonth_start, lastmonth_end, lastmonth


def request_report(token, startdate, enddate, offset):
    """
    If successful returns report request ID
    :param token: API token
    :param startdate: start date as a string in "YYYY-MM-DD" format
    :param enddate: end date as a string in "YYYY-MM-DD" format
    :return: request ID
    """

    # Send POST requesting Client IP report within specified dates
    request_header = {'Content-type': 'application/json', 'Authorization': 'Bearer ' + token}

    reporturl = 'https://api.ultradns.com/reports/dns_resolution/' \
                'directional_response_counts/class_c_network?offset={}&limit=10000'.format(offset)

    reportbody = json.dumps({
        'classCNetworkDirectionalResponseCounts': {
            'accountName': account_name,
            'reportStartDate': startdate,
            'reportEndDate': enddate
        }
    })

    try:
        response = requests.post(reporturl, data=reportbody, headers=request_header)
        print(response.json())
        requestid = response.json()['requestId']
        print("Report ID is {}, checking status...".format(requestid))
    except requests.exceptions.RequestException as e:
        print(e)

    return requestid


def get_report(token, requestid, startdate, offset):
    """
    Dumps report into csv file. Maximum limit established by UltraDNS is 10000 records.
    """

    # Keep checking status of the report, until status code is '200'.
    report_header = {'Content-type': 'application/json', 'Accept': 'text/csv', 'Authorization': 'Bearer ' + token}

    reporturl = 'https://api.ultradns.com/requests/' + requestid
    status = True
    while status:
        report_response = requests.get(reporturl, headers=report_header)
        if report_response.status_code != 200:
            print("Report is NOT ready, checking again in 10 seconds...")
            time.sleep(10)
        else:
            lastmonth = datetime.datetime.strptime(startdate, '%Y-%m-%d').strftime("%b_%Y")
            filename = "{}_report_{}.csv".format(lastmonth, offset)
            with open(filename, 'w') as csvfile:
                csvfile.write(report_response.text)
            print("Report is ready writing into " + filename + " file..")
            status = False


def combine_report(token, lastmonth_start, lastmonth_end):
    """
    API has limit of 10000 records per request, thus reports need to be combined.
    """

    limit = 50000

    # Request reports with offset up to limit.
    offset = 0
    while offset <= limit:
        request_id = request_report(token, lastmonth_start, lastmonth_end, offset)
        get_report(token, request_id, lastmonth_start, offset)
        offset += 10000

    # Concatenate all reports within the same month with different offsets into one file using Pandas dataframe
    lastmonth = datetime.datetime.strptime(lastmonth_start, '%Y-%m-%d').strftime("%b_%Y")
    filelist = []
    for file in os.listdir('./'):
        if file.startswith(lastmonth):
            filelist.append(file)
    df = pd.concat((pd.read_csv(f) for f in filelist))
    [os.remove(file) for file in filelist]
    df.to_csv(lastmonth + '_report.csv')


def consolidate_monthly_data(month):
    """
    Consolidates all query count by Country into one dataframe
    :param month:
    :return:
    """

    # Load dataframe from monthly report and drop unnecessary columns
    month_name=month.split('-')
    df = pd.read_csv(month_name[0] + '_' + month_name[1] + '_report.csv', index_col=0)
    df = df.drop(['Account Name', 'Report Start Date', 'Report End Date', 'Class C Network',
                  'City', 'Region', 'Authoritative DNS Node'], axis=1)

    # Create a new column with total queries per country
    df['Query Count - ' + month] = df.groupby(['Country'])['Total Response Count'].transform('sum')

    # Summarize all queries with 'null' Country
    df.loc[df['Country'].isnull(), 'Query Count - ' + month] = df.loc[df['Country'].isnull(), 'Total Response Count'].sum()

    # Drop all duplicates in 'Country' Column
    df = df.drop_duplicates(subset=['Country'])

    # Drop 'Total Response Count' column as we keep summary in a separate 'Query Count - Month' column
    df = df.drop(['Total Response Count'], axis=1)

    # Find out all queries from Uknown countries and distribute them across other countries proportionally.
    # Example, if United States has 80% of all queries - it will get 80% of all Uknown countries.
    total_unknown = df.loc[df['Country'].isnull(), 'Query Count - ' + month].sum()
    response_sum = df.loc[df['Country'].notnull()].sort_values(by=['Query Count - ' + month], ascending=False)['Query Count - ' + month].sum()
    for index, row in df.loc[df['Country'].notnull()].sort_values(by=['Query Count - ' + month], ascending=False).iterrows():
        newvalue = int(row['Query Count - ' + month] + ((row['Query Count - ' + month] / response_sum) * total_unknown))
        df.loc[index, 'Query Count - ' + month] = newvalue

    # Drop rows with unknown countries.
    df = df.dropna()

    df.reset_index(drop=True, inplace=True)
    df['Country'] = df['Country'].str.title()
    df = df.set_index('Country')

    return df


def dataframe_generator():
    """
    Combine all monthly reports into dataframe.
    """

    # Clean up old reports
    now = time.time()
    for file in os.listdir('./'):
        ctime = os.path.getctime(file)
        if (now - ctime) // (24 * 3600 ) >= 140 and file.endswith(".csv"):
            os.unlink(file)

    # Initialize a list of months.
    initlist = []
    for file in os.listdir('./'):
        if file.endswith(".csv"):
            month = file.split('_')
            initlist.append(datetime.datetime.strptime(month[0] + '-' + month[1], '%b-%Y'))
    initlist.sort()

    monthlist = []
    for month in initlist:
        monthlist.append(month.strftime('%b-%Y'))

    # Concatenate each monthly column to the right
    df = consolidate_monthly_data(monthlist[0])
    for month in monthlist[1::]:
        dt = consolidate_monthly_data(month)
        df = pd.concat([df, dt], axis=1, sort=False, join='inner')

    return df


def world_map_report(df, month):
    """
    Build a world map
    """
    country_geo = os.path.join('world-countries.json')

    df.reset_index(inplace=True)
    map_dict = df.set_index('Country')['Query Count - ' + month].to_dict()
    color_scale = cl.LinearColormap(['yellow', 'red'], vmin=min(map_dict.values()), index=[0, 2 * 10 ** 7],
                                    vmax=max(map_dict.values()))
    color_legend = cl.LinearColormap(['yellow', 'red'], vmin=min(map_dict.values()), index=[0, 10 ** 7], vmax=10 ** 7)
    color_legend.caption = 'DNS Query Count - By Source Country - ' + month

    def get_color(feature):
        value = map_dict.get(feature['properties']['name'])
        if value is None:
            return '#ffffff'  # MISSING -> white
        else:
            return color_scale(value)

    m = folium.Map(
        location=[30, 10],
        zoom_start=2
    )

    m.add_child(color_legend)

    folium.GeoJson(
        data=country_geo,
        style_function=lambda feature: {
            'fillColor': get_color(feature),
            'fillOpacity': 0.7,
            'color': 'black',
            'weight': 1,
        }
    ).add_to(m)

    m.save(outfile="world_map.html", zoom_start=2)


def diag_table_report(df, month):
    """
    Create a diagram and table of DNS Query Country by source country.
    """

    df_top10 = df.sort_values(by=['Query Count - ' + month], ascending=False).head(9)
    df_other = df[~df.isin(df_top10)].dropna()

    initlist = []
    for file in os.listdir('./'):
        if file.endswith(".csv"):
            month = file.split('_')
            initlist.append(datetime.datetime.strptime(month[0] + '-' + month[1], '%b-%Y'))
    initlist.sort()

    monthlist = []
    for month in initlist:
        monthlist.append(month.strftime('%b-%Y'))


    other = [df_other['Query Count - ' + i].sum() for i in monthlist]
    other.insert(0, 'Other')

    df_top10.loc[-1] = other
    df_top10 = df_top10.set_index('Country')
    df_top10_sorted = df_top10.sort_values(by=['Query Count - ' + monthlist[-1]], ascending=False).head(7)
    plot = df_top10_sorted.plot(kind='pie', y='Query Count - ' + monthlist[-1],
                                explode = (0, 0.2, 0.3, 0.3, 0.5, 0.8, 0.8),
                                title='DNS Query Count - By Source Country - ' + monthlist[-1],
                                figsize=(10, 10), legend=True, autopct='%.2f%%')

    fig = plot.get_figure()
    fig.tight_layout()
    fig.savefig("diag.png", orientation='landscape')

    df_top10.reset_index(inplace=True)
    total = [df['Query Count - ' + i].sum() for i in monthlist]
    total.insert(0, 'Total')
    df_top10.loc[-2] = total

    def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=14, header_color='#40466e',
                         row_colors=['#f1f1f2', 'w'], edge_color='w', bbox=[0, 0, 1, 1],
                         header_columns=0, ax=None, **kwargs):
        if ax is None:
            size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
            fig, ax = plt.subplots(figsize=size)
            ax.axis('off')

        mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

        mpl_table.auto_set_font_size(False)
        mpl_table.set_fontsize(font_size)

        for k, cell in six.iteritems(mpl_table._cells):
            cell.set_edgecolor(edge_color)
            if k[0] == 0 or k[1] < header_columns:
                cell.set_text_props(weight='bold', color='w')
                cell.set_facecolor(header_color)
            else:
                cell.set_facecolor(row_colors[k[0] % len(row_colors)])

        plt.savefig("table.png")

    fig = render_mpl_table(df_top10, col_width=3.5)


def send_mail(send_from, send_to, month):
    subject = "DNS Query Report for " + month
    body = "Find attached DNS report."

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = send_from
    message["To"] = send_to
    message["Subject"] = subject

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    files_to_send = []
    for file in os.listdir('./'):
        if file.endswith(".html") or file.endswith(".png"):
            files_to_send.append(file)

    # Open files in binary mode
    for filename in files_to_send:
        with open(filename, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )

        # Add attachment to message and convert message to string
        message.attach(part)

    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    try:
        server = smtplib.SMTP(smtp_server, 587)
        server.starttls(context=context)
        server.sendmail(send_from, send_to, text)
    except Exception as e:
        print(e)
    finally:
        server.quit()


def main(argv):

    token = get_token()
    lastmonth = get_month()

    combine_report(token, lastmonth[0], lastmonth[1])

    df = dataframe_generator()

    world_map_report(df, lastmonth[2])
    diag_table_report(df, lastmonth[2])

    mail_to = ''

    try:
        opts, args = getopt.getopt(argv, "hm:", ["mailto="])
    except getopt.GetoptError:
        print("ultradns-client.py -m <receiver email>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("ultradns-client.py -m <receiver email>")
            sys.exit(0)
        elif opt in ("-m", "--mailto"):
            mail_to = arg

    if mail_to == '':
        print(sys.argv[0], "-m <receiver email> (Parameter missing)")
        sys.exit(3)

    send_mail(sender_email, mail_to, lastmonth[2])


if __name__ == '__main__':

    main(sys.argv[1:])
