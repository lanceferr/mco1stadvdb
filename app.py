import io
import base64
from flask import Flask, render_template, request
from sqlalchemy import create_engine
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

app = Flask(__name__)

DB_CONFIG = {
    "user": "root",
    "password": "Growtopia1122",
    "host": "localhost",
    "port": 3306,
    "database": "transaction"
}

engine = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                       f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/slice', methods=['GET', 'POST'])
def slice():
    regions = pd.read_sql("SELECT DISTINCT region_name FROM dim_region;", engine)

    if request.method == 'POST':
        region = request.form['region']

        query = f"""
        SELECT r.region_name, dd.year, dd.month, AVG(f.amount) AS average_amount
        FROM fact_transaction f
        JOIN dim_account a ON f.account_id = a.account_id
        JOIN dim_district d ON a.district_id = d.district_id
        JOIN dim_region r ON d.region_id = r.region_id
        JOIN dim_date dd ON f.date_key = dd.date_key
        WHERE r.region_name = '{region}'
        GROUP BY dd.year, dd.month
        ORDER BY dd.year, dd.month;
        """
        df = pd.read_sql(query, engine)

        df['year_month'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')

        plt.figure(figsize=(10,5))
        plt.plot(df['year_month'], df['average_amount'], marker='o', color='teal')
        plt.xlabel('Month')
        plt.ylabel('Average Transaction Amount')
        plt.title(f'Average Transaction Trend in {region}')
        plt.gcf().autofmt_xdate()
        plt.tight_layout()

        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        chart = base64.b64encode(img.getvalue()).decode()
        plt.close()

        return render_template(
            'slice.html',
            regions=regions['region_name'],
            selected_region=region,
            chart=chart
        )

    return render_template('slice.html', regions=regions['region_name'])



@app.route('/dice', methods=['GET', 'POST'])
def dice():
    #for dropdown
    regions = pd.read_sql("SELECT DISTINCT region_name FROM dim_region;", engine)
    years = pd.read_sql("SELECT DISTINCT year FROM dim_date;", engine)

    if request.method == 'POST':
        region = request.form['region']
        year = request.form['year']

        #average transaction amount by district and transaction type
        query = f"""
        SELECT d.district_name, t.type, AVG(f.amount) AS average_amount
        FROM fact_transaction f
        JOIN dim_account a ON f.account_id = a.account_id
        JOIN dim_district d ON a.district_id = d.district_id
        JOIN dim_region r ON d.region_id = r.region_id
        JOIN dim_date dd ON f.date_key = dd.date_key
        JOIN dim_transtype t ON f.transtype_id = t.transtype_id
        WHERE r.region_name = '{region}' AND dd.year = {year}
        GROUP BY d.district_name, t.type
        ORDER BY d.district_name;
        """
        df = pd.read_sql(query, engine)

        pivot_df = df.pivot(index='district_name', columns='type', values='average_amount').fillna(0)
        pivot_df.plot(kind='bar', stacked=True, figsize=(12,6), colormap='tab20')
        plt.xlabel('District')
        plt.ylabel('Average Transaction Amount')
        plt.title(f'Average Transaction Amount by District and Type in {region} ({year})')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        chart = base64.b64encode(img.getvalue()).decode()
        plt.close()

        return render_template(
            'dice.html',
            regions=regions['region_name'],
            years=years['year'],
            selected_region=region,
            selected_year=year,
            chart=chart
        )

    return render_template('dice.html', regions=regions['region_name'], years=years['year'])

@app.route('/rollup', methods=['GET', 'POST'])
def rollup():
    level = request.form.get('level', 'quarter')

    if level == 'quarter':
        query = """
        SELECT dd.year, dd.quarter, SUM(f.amount)/100000 AS total_amount
        FROM fact_transaction f
        JOIN dim_date dd ON f.date_key = dd.date_key
        GROUP BY dd.year, dd.quarter
        ORDER BY dd.year, dd.quarter;
        """
    else:  #yearly rollup
        query = """
        SELECT dd.year, SUM(f.amount)/100000 AS total_amount
        FROM fact_transaction f
        JOIN dim_date dd ON f.date_key = dd.date_key
        GROUP BY dd.year
        ORDER BY dd.year;
        """

    df = pd.read_sql(query, engine)

    if level == 'quarter':
        labels = df['year'].astype(str) + " Q" + df['quarter'].astype(str)
        values = df['total_amount']
        title = "Transaction Amounts by Quarter (x100000)"
    else:
        labels = df['year'].astype(str)
        values = df['total_amount']
        title = "Transaction Amounts by Year (x100000)"

    plt.figure(figsize=(10,5))
    plt.bar(labels, df['total_amount'], color='skyblue')
    plt.xlabel('Time')
    plt.ylabel('Total Amount (x100000)')
    plt.title(title)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    chart = base64.b64encode(img.getvalue()).decode()
    plt.close()

    return render_template('rollup.html', chart=chart, selected_level=level)


@app.route('/drilldown', methods=['GET', 'POST'])
def drilldown():
    #for dropdown
    regions = pd.read_sql("SELECT DISTINCT region_name FROM dim_region;", engine)

    selected_region = request.form.get('region')
    selected_district = request.form.get('district')

    districts = []

    if selected_region and not selected_district: #SELECTED A REGION
        #for dropdown
        districts_df = pd.read_sql(f"""
            SELECT DISTINCT district_name
            FROM dim_district d
            JOIN dim_region r ON d.region_id = r.region_id
            WHERE r.region_name = '{selected_region}'
        """, engine)
        districts = districts_df['district_name'].tolist()

        query = f"""
        SELECT d.district_name, SUM(f.amount) AS total_amount
        FROM fact_transaction f
        JOIN dim_account a ON f.account_id = a.account_id
        JOIN dim_district d ON a.district_id = d.district_id
        JOIN dim_region r ON d.region_id = r.region_id
        WHERE r.region_name = '{selected_region}'
        GROUP BY d.district_name
        ORDER BY total_amount DESC;
        """
        df = pd.read_sql(query, engine)

        plt.figure(figsize=(10,5))
        plt.bar(df['district_name'], df['total_amount'], color='skyblue')
        plt.xlabel('District')
        plt.ylabel('Total Amount')
        plt.title(f'Total Transactions by District in {selected_region}')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

    elif selected_region and selected_district: #ACCOUTNS UNDER THE DISTRICT
        query = f"""
        SELECT a.account_id, SUM(f.amount) AS total_amount, a.transaction_count, a.frequency
        FROM fact_transaction f
        JOIN dim_account a ON f.account_id = a.account_id
        JOIN dim_district d ON a.district_id = d.district_id
        JOIN dim_region r ON d.region_id = r.region_id
        WHERE r.region_name = '{selected_region}' AND d.district_name = '{selected_district}'
        GROUP BY a.account_id, a.transaction_count, a.frequency
        ORDER BY total_amount DESC;
        """
        df = pd.read_sql(query, engine)
        table_html = df.to_html(classes='table table-striped', index=False)

    else: #ALL REGIONS
        query = """
        SELECT r.region_name, SUM(f.amount) AS total_amount
        FROM fact_transaction f
        JOIN dim_account a ON f.account_id = a.account_id
        JOIN dim_district d ON a.district_id = d.district_id
        JOIN dim_region r ON d.region_id = r.region_id
        GROUP BY r.region_name
        ORDER BY total_amount DESC;
        """
        df = pd.read_sql(query, engine)

        plt.figure(figsize=(10,5))
        plt.bar(df['region_name'], df['total_amount'], color='lightgreen')
        plt.xlabel('Region')
        plt.ylabel('Total Amount')
        plt.title('Total Transactions by Region')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

    chart = None
    if 'df' in locals() and not (selected_region and selected_district):
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        chart = base64.b64encode(img.getvalue()).decode()
        plt.close()

    return render_template(
        'drilldown.html',
        regions=regions['region_name'],
        districts=districts,
        selected_region=selected_region,
        selected_district=selected_district,
        chart=chart,
        table_html=table_html if 'table_html' in locals() else None
    )


if __name__ == '__main__':
    app.run(debug=True)
