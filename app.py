import sqlite3
import numpy as np
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from datetime import date

# Connect to the database
conn = sqlite3.connect('olist_PDDS.sqlite')

# Graph 1 SQL query
sql_gr2 = """
SELECT seller_id, seller_city, won_date, business_segment
FROM sellers
NATURAL JOIN closed_deals
"""
gr2_df = pd.read_sql_query(sql_gr2, conn)
gr2_df['won_date'] = pd.to_datetime(gr2_df['won_date'], format='%d/%m/%Y %H:%M')
gr2_df['month'] = gr2_df['won_date'].dt.to_period('M').astype(str)
fixed_months = pd.date_range(start='2018-01', end='2018-08', freq='ME').strftime('%Y-%m')

# Graph 2 SQL query
sql_gr1 = """
SELECT business_segment, won_date, seller_ID
FROM closed_deals
"""
gr1_df = pd.read_sql_query(sql_gr1, conn)
# Converting won_date column from text to datetime
gr1_df['won_date'] = pd.to_datetime(gr1_df['won_date'])
# Converting to a datetime object
gr1_df['month'] = gr1_df['won_date'].dt.strftime('%Y-%m')
# Filter out months before January 2018
gr1_df = gr1_df[gr1_df['month'] >= '2017-12-31']
# Group by
month_counts = gr1_df.groupby(['business_segment', 'month']).size().reset_index(name='segment_count')
month_counts['rank'] = month_counts.groupby('month')['segment_count'].rank(method='first', ascending=False)
top_10_month_counts = month_counts[month_counts['rank'] <= 10]

# Graph 3 SQL query
closed_deals = pd.read_sql_query("SELECT * FROM closed_deals", conn)
sellers = pd.read_sql_query("SELECT * FROM sellers", conn)
sellers['is_new_seller'] = sellers['seller_id'].isin(closed_deals['seller_id'])
state_summary = (
    sellers.groupby('seller_state')
    .agg(
        new_sellers=('is_new_seller', 'sum'),  # Count of new sellers
        old_sellers=('is_new_seller', lambda x: (~x.astype(bool)).sum())  # Count of old sellers (not in closed_deals)
    )
    .reset_index()
)
state_summary['total_sellers'] = state_summary['new_sellers'] + state_summary['old_sellers']
# Rename states
state_name_mapping = {
    'SP': 'São Paulo (SP)', 'RJ': 'Rio de Janeiro (RJ)', 'MG': 'Minas Gerais (MG)', 'RS': 'Rio Grande do Sul (RS)', 'BA': 'Bahia (BA)',
    'CE': 'Ceará (CE)', 'DF': 'Distrito Federal (DF)', 'ES': 'Espírito Santo (ES)', 'GO': 'Goiás (GO)', 'PB': 'Paraíba (PB)',
    'PE': 'Pernambuco (PE)', 'PR': 'Paraná (PR)', 'SC': 'Santa Catarina (SC)', 'AC': 'Acre (AC)', 'AM': 'Amazonas (AM)',
    'MA': 'Maranhão (MA)', 'MS': 'Mato Grosso do Sul (MS)', 'MT': 'Mato Grosso (MT)', 'PA': 'Pará (PA)', 'PI': 'Piauí (PI)', 'RN': 'Rio Grande do Norte (RN)',
    'RO': 'Rondônia (RO)', 'SE': 'Sergipe (SE)'
}
state_summary['seller_state'] = state_summary['seller_state'].map(state_name_mapping).fillna('Unknown State')

#Graph 4
#`closed_deals`` table is already connected
#Need to transform the closed_deals table in order to merge it with the order table later on
#Transform the won_date column as datetime and parse it to date only
closed_deals['won_date'] = pd.to_datetime(closed_deals['won_date'], dayfirst = True).dt.normalize()

#Creating DataFrame from the `order` table
order = pd.read_sql_query("SELECT * FROM order_2", conn)
conn.close()

#Create SQL Query to filter order table, in order to containing orders from seller_id available in closed_deals data
#Or in other words, filter the order table to only contain the new seller's orders data.
order_new_seller = order[order['seller_id'].isin(closed_deals['seller_id'])]

#Filter the order with the status delivered
order_new_seller = order_new_seller[order_new_seller['status'] == 'delivered']

#Copy the won_date in closed_deals to order_new_seller table to mark the date the seller joins
#Using left join
order_new_seller = order_new_seller.merge(
    closed_deals[['seller_id', 'won_date']],
    on = 'seller_id',
    how = 'left'
)

#Transform the order_purchase_timestamp column into date only format
order_new_seller['order_purchase_timestamp'] = pd.to_datetime(order_new_seller['order_purchase_timestamp'], format = "%d/%m/%Y %H:%M").dt.normalize()

#Drop rows because in order_delivered_customer_date has some missing values
order_new_seller = order_new_seller.drop(order_new_seller[order_new_seller['order_delivered_customer_date'] == "00/01/1900 00:00"].index)

#Transform the order_delivered_customer_date column into date only format
order_new_seller['order_delivered_customer_date'] = pd.to_datetime(order_new_seller['order_delivered_customer_date'], format = '%d/%m/%Y %H:%M').dt.normalize()

#Adding new column to mark the month when the seller join Olist (to be used in Dash)
order_new_seller['join_month'] = order_new_seller['won_date'] + pd.offsets.MonthEnd(0)

#Create a dummy filter date
filter_date = "2018-06-30"
filter_date = np.datetime64(pd.to_datetime(filter_date))

#Create a filter for seller age category
filter_age_category = '1 month'

# Create new column to store the age of the seller based on the filter date
order_new_seller['Seller age as of threshold date'] = (filter_date - order_new_seller['won_date']).dt.days

#Create new column to group the age of the seller based on the threshold date
order_new_seller['age_category'] = order_new_seller['Seller age as of threshold date'].apply(
    lambda x:
        "Not joining yet" if x <= 0 else #if the seller joins on the same day with the filter date, we assume there will be no orders yet.
        "1 month" if x <= 30 else
        "2 months" if x <= 60 else
        "3 months" if x <= 90 else
        "More than 3 months"
)

#Creating a new column to mark if the transaction has happened or not based on the filter date
order_new_seller['trx_happened'] = order_new_seller['order_purchase_timestamp'].apply(
    lambda x:
    "Trx has happened" if x < filter_date else
    "Trx hasn't happened")

#Create a column to store how old the seller was when the transaction happened (order_purchase_timestamp - won_date)
order_new_seller['transaction_age'] = order_new_seller['order_purchase_timestamp'] - order_new_seller['won_date']
order_new_seller['transaction_age'] = order_new_seller['transaction_age'].dt.days

#Create a column to group how old the seller was when the transaction happened (based on transaction_age)
order_new_seller['transaction_age_mark'] = order_new_seller['transaction_age'].apply(
    lambda x:
        "Month 1" if x <= 30 else
        "Month 2" if x <= 60 else
        "Month 3" if x <= 90 else
        "Old seller"
)

#Create a dummy data for showing empty chart if there is no data in the pivot table
dummy = pd.DataFrame({'x': [0], 'y': [0]})


# Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# For deployment
server = app.server

app.layout = html.Div(
    style={
        'backgroundColor': '#F8F9FA', 
        'padding': '20px'
    },
    children=[
        html.H1(
            "Olist Seller Performance Analysis", 
            style={'fontFamily': 'Arial, sans-serif', 'fontSize': '35px', 'textAlign': 'center', 'margin': '20', 'fontWeight': 'bold', 'marginTop': '-10px'}
        ),      
        dbc.Row(
            children=[
                # Graph 1 - Monthly Counts of Sellers by Business Segment
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            children=[
                                html.H2("Monthly New Sellers per City", style={'textAlign': 'center', 'fontFamily': 'Arial, sans-serif', 'fontSize': '20px'}),
                                html.Div([
                                    dcc.Dropdown(
                                        id='city-filter',
                                        options=[{'label': city, 'value': city} for city in gr2_df['seller_city'].unique()],
                                        value=None,
                                        placeholder="Select a city",
                                        style={'marginBottom': '10px', 'marginRight': '20px', 'marginLeft': '15px'}
                                    )
                                ], style={'width': '48%', 'display': 'inline-block'}),                     
                                html.Div([
                                    dcc.Dropdown(
                                        id='segment-filter',
                                        options=[{'label': segment, 'value': segment} for segment in gr2_df['business_segment'].unique()],
                                        value=None,
                                        placeholder="Select a business segment",
                                        style={'marginBottom': '10px', 'marginRight': '0px', 'marginLeft':'0px'}
                                    )
                                ], style={'display':'none'}),
                                dcc.Graph(id='line-chart')
                            ]
                        ),
                        style={
                            'padding': '5px',
                            'border': '1px solid #E9ECEF',
                            'borderRadius': '8px',
                        }
                    ),
                    width=6
                ), 

                # Graph 2
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody(
                            children=[
                                html.H2("Top 10 Business Segments of New Sellers", style={'fontFamily': 'Arial, sans-serif', 'fontSize': '20px', 'textAlign': 'center'}),
                                html.Div(
                                    dcc.Dropdown(
                                        id='catsel-dropdown',
                                        options=[{'label': date, 'value': date} for date in sorted(month_counts['month'].unique())],
                                        value=None,
                                        multi=False,
                                        placeholder='Select month',
                                        style={'marginBottom': '10px', 'marginRight': '30px', 'marginLeft': '0px'}
                                    ),
                                    style={'width': '48%', 'display': 'inline-block'}
                                ),
                                html.Div(
                                    dcc.Dropdown(
                                        id='segment-dropdown',
                                        options=[{'label': segment, 'value': segment} for segment in sorted(month_counts['business_segment'].unique())],
                                        value=None,
                                        multi=False,
                                        placeholder='Select Business Segment',
                                        style={'marginBottom': '10px', 'marginRight': '0px', 'marginLeft':'20px'}
                                    ),
                                    style={'width': '48%', 'display': 'inline-block'}
                                ),
                                dcc.Graph(id='catsel-chart'),
                                dcc.Graph(id='segment-line-chart', style={'display': 'none'})
                            ]
                        ),
                        style={
                            'padding': '5px',
                            'border': '1px solid #E9ECEF',
                            'borderRadius': '8px',
                            'height': '628px'
                        }
                    ),
                    width=6
                ),  
            ]
        ),
        html.Br(),
        # Graph 3 - Seller Distribution Overview
        dbc.Row(
            children=[
                dbc.Col(
                    html.Div(
                        style={'fontFamily': 'Arial, sans-serif', 'padding': '20px', 'backgroundColor': '#F8F9FA'},
                        children=[
                            html.H1(
                                "Seller Distribution Overview",
                                style={'textAlign': 'center', 'color': '#343A40', 'fontSize': '25px', 'marginBottom': '20px'}
                            ),
                            html.Div(
                                children=[
                                    html.P(
                                        "Data reflects sellers active (who made at least one sale) between January 02, 2017 and September 03, 2018.",
                                        style={'textAlign': 'center', 'color': '#495057', 'fontSize': '18px', 'marginBottom': '20px'}
                                    ),
                                ]
                            ),
                            dcc.Dropdown(
                                id='state-dropdown',
                                options=[
                                    {'label': state, 'value': state} for state in state_summary['seller_state']
                                ],
                                placeholder='Select one or more States for Comparison',
                                multi=True,
                                style={'marginBottom': '20px', 'width': '60%', 'marginLeft': 'auto', 'marginRight': 'auto'}
                            ),
                            html.Div(
                                style={'display': 'flex', 'justifyContent': 'space-between', 'gap': '20px'},
                                children=[
                                    # Gradient Bar Chart
                                    html.Div(
                                        style={'flex': '1', 'padding': '10px', 'border': '1px solid #E9ECEF', 'borderRadius': '8px'},
                                        children=[
                                            html.H2("Seller Distribution by State", style={'textAlign': 'center', 'color': '#343A40', 'fontSize': '20px'}),
                                            dcc.Graph(
                                                id='state-gradient-chart',
                                                config={'displayModeBar': False}
                                            )
                                        ]
                                    ),
                                    # Sellers by Type Chart
                                    html.Div(
                                        style={'flex': '1', 'padding': '10px', 'border': '1px solid #E9ECEF', 'borderRadius': '8px'},
                                        children=[
                                            html.H2("New vs Old Sellers", style={'textAlign': 'center', 'color': '#343A40', 'fontSize': '20px'}),
                                            dcc.Graph(
                                                id='sellers-bar-chart',
                                                config={'displayModeBar': False}
                                            )
                                        ]
                                    ),
                                ]
                            ),
                            html.Div(
                                id='state-info',
                                style={
                                    'marginTop': '20px',
                                    'padding': '10px',
                                    'border': '1px solid #E9ECEF',
                                    'borderRadius': '8px',
                                    'backgroundColor': '#FFFFFF',
                                    'textAlign': 'center',
                                    'color': '#343A40',
                                    'fontSize': '18px'
                                }
                            )
                        ]
                    ),
                    width=12
                )
            ]
        ),
        html.Br(),
        # Graph 4
        dbc.Row(
            children=[
                dbc.Col(
                    html.Div([
                        html.H1(
                            "New Sellers Performance",
                            style={'textAlign': 'center', 'fontFamily': 'Arial, sans-serif', 'fontSize': '25px', 'marginBottom': '20px'}
                            ),
                        html.Label("Select Cut-Off Month: "),
                        dcc.Dropdown(
                            id='order-month',
                            options=[{'label': 'Jan 2018', 'value': '2018-01-31'},
                                     {'label': 'Feb 2018', 'value': '2018-02-28'},
                                     {'label': 'Mar 2018', 'value': '2018-03-31'},
                                     {'label': 'Apr 2018', 'value': '2018-04-30'},
                                     {'label': 'May 2018', 'value': '2018-05-31'},
                                     {'label': 'Jun 2018', 'value': '2018-06-30'},
                                     {'label': 'Jul 2018', 'value': '2018-07-31'},
                                     {'label': 'Aug 2018', 'value': '2018-08-31'},
                                     {'label': 'Sep 2018', 'value': '2018-09-30'},
                                     {'label': 'Oct 2018', 'value': '2018-10-31'},
                                     {'label': 'Nov 2018', 'value': '2018-11-30'},
                                     {'label': 'Dec 2018', 'value': '2018-12-31'}],
                            placeholder="Select Month",
                            value='2018-01-31',
                            multi=False
                        ),
                        html.Label('Select Seller Age Category:'),
                        dcc.Dropdown(
                            id='seller-age',
                            options=[{'label': '1 Month', 'value': '1 month'},
                                     {'label': '2 Months', 'value': '2 months'},
                                     {'label': '3 Months', 'value': '3 months'}],
                            placeholder="Select Age Category",
                            value='1 month',
                            multi=False
                        ),
                        html.Br(),
                        # 1st Row
                        html.H3(
                            "Top Performing New Sellers",
                            style={'textAlign': 'center', 'fontFamily': 'Arial, sans-serif', 'fontSize': '20px', 'marginBottom': '20px'}),  
                        html.Div([
                            dcc.Graph(id='sales-sum'),
                            dcc.Graph(id='sales-count')],
                            style={
                                'display': 'flex',
                                'align-items': 'flex-end',
                                'margin-bottom': '20px'
                        }),
                        # 2nd Row
                        html.H3("Lowest Performing New Sellers",
                                style={'textAlign': 'center', 'fontFamily': 'Arial, sans-serif', 'fontSize': '20px', 'marginBottom': '20px'}),  
                        html.Div([
                            dcc.Graph(id = 'sales-sum-2'),
                            dcc.Graph(id = 'sales-count-2')],
                            style = {
                            'display': 'flex',
                            'align-items': 'flex-end',
                            'margin-bottom': '20px'
    }),
                        # 3rd Row
                        html.H2("Sales Trend of New Sellers",
                                style={'textAlign': 'center', 'fontFamily': 'Arial, sans-serif', 'fontSize': '25px', 'marginBottom': '20px'}),
                        html.Label('Select Seller ID:'),
                        dcc.Dropdown(
                            id='seller-id',
                            options=[{'label': seller_id, 'value': seller_id} for seller_id in order_new_seller['seller_id'].unique()],
                            placeholder="Select Seller ID",
                            multi=True
                        ),
                        html.Div([
                            dcc.Graph(id='sales-sum-trend'),
                            dcc.Graph(id='sales-count-trend')],
                            style={
                                'display': 'flex',
                                'align-items': 'flex-end',
                                'margin-bottom': '20px'
                        })
                    ])
                )
            ]
        )
    ]
)

# App Callbacks

# App Callback 1
@app.callback(
    Output('line-chart', 'figure'),
    [Input('city-filter', 'value'),
     Input('segment-filter', 'value')]
)
def update_chart(selected_city, selected_segment):
    # Filter the DataFrame based on the selected filters
    filtered_df = gr2_df.copy()
    if selected_city:
        filtered_df = filtered_df[filtered_df['seller_city'] == selected_city].copy()
    if selected_segment:
        filtered_df = filtered_df[filtered_df['business_segment'] == selected_segment].copy()

    # Group by month to count the number of unique sellers
    monthly_data = (
        filtered_df.groupby('month')['seller_id']
        .nunique()
        .reset_index(name='num_sellers')
    )

    # Ensure all months are included
    all_months = pd.date_range(start='2018-01-01', end='2018-08-31', freq='ME').strftime('%Y-%m').tolist()
    monthly_data = monthly_data.set_index('month').reindex(all_months, fill_value=0).reset_index()
    monthly_data.columns = ['month', 'num_sellers']

    # Determine the y-axis range
    max_sellers = monthly_data['num_sellers'].max()
    yaxis_range = [0, max_sellers + 5] if not selected_city and not selected_segment else [0, max_sellers + 1]


    # Create the line chart
    fig = px.line(
        monthly_data,
        x='month',
        y='num_sellers',
        title='Number of Sellers Over Time',
        labels={'month': 'Month', 'num_sellers': 'Number of Sellers'},
        markers=True
    )

    # Update layout with fixed integer ticks
    fig.update_layout(
        xaxis=dict(
            categoryorder='array',
            categoryarray=all_months,  # Ensure the order of months is consistent
            title='Month'
        ),
        yaxis=dict(
            title='Number of Sellers',
            tick0=0,
            range=yaxis_range  # Dynamic range
        ),
        margin=dict(l=40, r=40, t=40, b=80),
        height=500,  # Increase height to avoid compression
    )

    return fig

# App Callback 2

@app.callback(
    [Output('catsel-chart', 'figure'),
     Output('segment-line-chart', 'figure'),
     Output('catsel-chart', 'style'),
     Output('segment-line-chart', 'style')],
    [Input('catsel-dropdown', 'value'),
     Input('segment-dropdown', 'value')]
)
def update_charts(selected_month, selected_segment):
    if selected_segment:
        filtered_line_df = gr1_df[gr1_df['business_segment'] == selected_segment]
        line_fig = px.line(
            filtered_line_df.groupby(['month', 'business_segment']).size().reset_index(name='sales_count'),
            x='month', y='sales_count', color='business_segment',
            title=f"Monthly Sales for {selected_segment}",
            labels={'month': 'Month', 'sales_count': 'Number of Sales'}
        )
        line_fig.update_layout(showlegend=False)
        bar_chart_style = {'display': 'none'}
        line_chart_style = {'display': 'block'}
        return {}, line_fig, bar_chart_style, line_chart_style
    
    else:
        if selected_month:
            filtered_df = top_10_month_counts[top_10_month_counts['month'] == selected_month]
        else:
            filtered_df = top_10_month_counts

        bar_fig = px.bar(
            filtered_df,
            x='month',
            y='segment_count',
            color='business_segment',
            barmode='group',
            labels={'month': 'Month', 'segment_count': 'Number of Products Sold', 'business_segment': 'Business Segment'}
        )
        bar_fig.update_layout(
            margin=dict(l=5, r=5, t=15, b=1),
            xaxis=dict(
                tickformat='%Y-%m')),
        bar_fig.update_layout(showlegend=False)

        line_chart_style = {'display': 'none'}
        bar_chart_style = {'display': 'block'}
        return bar_fig, {}, bar_chart_style, line_chart_style

# App Callback 3
# Update the gradient chart when states are selected or deselected
@app.callback(
    Output('state-gradient-chart', 'figure'),
    Input('state-dropdown', 'value')
)
def update_gradient_chart(selected_states):
    state_summary_filtered = state_summary[state_summary['seller_state'].isin(selected_states)] if selected_states else state_summary
    
    fig = px.bar(
        state_summary_filtered.sort_values(by="total_sellers", ascending=False),
        x='seller_state',
        y='total_sellers',
        color='total_sellers',
        color_continuous_scale='RdYlBu',
        labels={'seller_state': 'State', 'total_sellers': 'Total Sellers'},
        title='Seller Count by State'
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        title_font={'size': 20},
        font=dict(family='Arial', size=14),
        xaxis=dict(showgrid=False, title='States'),
        yaxis=dict(showgrid=True, title='Total Sellers'),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

# Update the bar chart for new vs old sellers when states are selected
@app.callback(
    Output('sellers-bar-chart', 'figure'),
    Input('state-dropdown', 'value')
)
def update_bar_chart(selected_states):
    filtered_data = state_summary if not selected_states else state_summary[state_summary['seller_state'].isin(selected_states)]
    fig = px.bar(
        filtered_data.melt(id_vars='seller_state', value_vars=['new_sellers', 'old_sellers']),
        x='seller_state',
        y='value',
        color='variable',
        barmode='group',
        labels={'seller_state': 'State', 'value': 'Number of Sellers', 'variable': 'Seller Type'},
        title='New vs Old Sellers by State',
        color_discrete_map={'new_sellers': '#B71C1C', 'old_sellers': '#0D47A1'}
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        title_font={'size': 20},
        font=dict(family='Arial', size=14),
        xaxis=dict(showgrid=False, title='States'),
        yaxis=dict(showgrid=True, title='Number of Sellers'),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

# Show selected states' information in text format
@app.callback(
    Output('state-info', 'children'),
    Input('state-dropdown', 'value')
)
def update_state_info(selected_states):
    if not selected_states:
        return "Select one or more states to view a summary of their performance."
    
    selected_data = state_summary[state_summary['seller_state'].isin(selected_states)]
    summary_lines = []
    
    for _, row in selected_data.iterrows():
        state_info = f"📍 {row['seller_state']}: Total Sellers: {row['total_sellers']}, "
        
        if row['new_sellers'] == 0:
            state_info += "New Sellers: 0 (No new sellers in this period), "
        else:
            state_info += f"New Sellers: {row['new_sellers']}, "
        
        if row['old_sellers'] == 0:
            state_info += "Old Sellers: 0 (No old sellers in this period)"
        else:
            state_info += f"Old Sellers: {row['old_sellers']}"
        
        summary_lines.append(state_info)
    
    return html.Ul([html.Li(line) for line in summary_lines])

# App Callback 4
@app.callback(
    [Output('sales-sum', 'figure'),
     Output('sales-count', 'figure'),
     Output('sales-sum-2', 'figure'),
     Output('sales-count-2', 'figure'),
     Output('sales-sum-trend', 'figure'),
     Output('sales-count-trend', 'figure'),
     #Output('dynamic-dropdown', 'options')
    ],
    [Input('order-month', 'value'),
     Input('seller-age', 'value'),
     Input('seller-id', 'value'),
     #Input('dynamic-dropdown', 'value')
    ]
)

def update_chart(selected_order_month, selected_seller_age, selected_seller_id):
    
    #Transform the cut-off month filter
    selected_order_month = np.datetime64(pd.to_datetime(selected_order_month)) #if selected_order_month else np.datetime64(pd.to_datetime('2018-01-01', format = "%Y-%m-%d"))
    
    #Filter data based on selected month in the dropdown
    order_new_seller['Seller age as of threshold date'] = (selected_order_month - order_new_seller['won_date']).dt.days

    order_new_seller['age_category'] = order_new_seller['Seller age as of threshold date'].apply(
        lambda x:
        "Not joining yet" if x <= 0 else #if the seller joins on the same day with the filter date, we assume there will be no orders yet.
        "1 month" if x <= 30 else
        "2 months" if x <= 60 else
        "3 months" if x <= 90 else
        "More than 3 months")

    #Pivot for sales amount based on age
    order_new_seller_sum = pd.pivot_table(
        order_new_seller[order_new_seller['age_category'] == selected_seller_age],
        values='amount',
        index='seller_id',
        aggfunc='sum')
    flat_order_sum = order_new_seller_sum.reset_index()

    #Pivot for number of sales based on age
    order_new_seller_count = pd.pivot_table(
        order_new_seller[order_new_seller['age_category'] == selected_seller_age],
        values='order_id',
        index='seller_id',
        aggfunc='count')
    flat_order_count = order_new_seller_count.reset_index()

    #Filter new_seller_growth_amount to include only selected seller id
    new_seller_growth_amount = pd.pivot_table(
        data=order_new_seller[(order_new_seller['trx_happened'] == 'Trx has happened') & (order_new_seller['age_category'] == filter_age_category)],
        values='amount',        
        index=['seller_id', 'transaction_age_mark'],       
        aggfunc='sum').fillna(0)
    
    new_seller_growth_amount = new_seller_growth_amount.reset_index()
    new_seller_growth_amount = new_seller_growth_amount.sort_values(by = 'transaction_age_mark', ascending = True)
    filtered_new_seller_growth_amount = new_seller_growth_amount[new_seller_growth_amount['seller_id'].isin(selected_seller_id)] if selected_seller_id else new_seller_growth_amount

    #Filter new_seller_growth_count to include only selected seller id
    new_seller_growth_count = pd.pivot_table(
        data=order_new_seller[(order_new_seller['trx_happened'] == 'Trx has happened') & (order_new_seller['age_category'] == filter_age_category)],
        values='order_id',        
        index=['seller_id', 'transaction_age_mark'],       
        aggfunc='count').fillna(0)

    new_seller_growth_count = new_seller_growth_count.reset_index()
    new_seller_growth_count = new_seller_growth_count.sort_values(by = 'transaction_age_mark', ascending = True)
    filtered_new_seller_growth_count = new_seller_growth_count[new_seller_growth_count['seller_id'].isin(selected_seller_id)] if selected_seller_id else new_seller_growth_count

    #Generate the sales-sum and sales-count bar chart
    if flat_order_sum.empty and flat_order_count.empty:
        #Using dummy dataframe to show empty chart if the pivot table is empty
        fig_sum = px.bar(dummy, x= 'x', y= 'y', title= "No Seller Data Available", labels = {"x": "Sales Amount (in Real Brazil)", "y": "Seller ID"})
        fig_count = px.bar(dummy, x= 'x', y= 'y', title= "No Seller Data Available", labels = {"x": "Number of Orders", "y": "Seller ID"})
        fig_sum_2 = px.bar(dummy, x= 'x', y= 'y', title= "No Seller Data Available", labels = {"x": "Sales Amount (in Real Brazil)", "y": "Seller ID"})
        fig_count_2 = px.bar(dummy, x= 'x', y= 'y', title= "No Seller Data Available", labels = {"x": "Number of Orders", "y": "Seller ID"})
        fig_trend_amount = px.line(dummy,
                                   x = "x",
                                   y = "y",
                                   title = "No Data Available",
                                   labels = {"x": "Month Age of Seller", "y": "Sales Amount (in Real Brazil)"},
                                   markers = True)
        fig_trend_count = px.line(dummy,
                                   x = "x",
                                   y = "y",
                                   title = "No Data Available",
                                   labels = {"x": "Month Age of Seller", "y": "Number of Orders"},
                                   markers = True)
        return fig_sum, fig_count, fig_sum_2, fig_count_2, fig_trend_amount, fig_trend_count
    else:
        #If the pivot table is not empty, then show the graph
        flat_order_sum = flat_order_sum.sort_values(by = 'amount', ascending = False) #sort by ascending value after making sure that the pivot table is not empty
        flat_order_count = flat_order_count.sort_values(by = 'order_id', ascending = False) #sort by ascending value after making sure that the pivot table is not empty
        top10_amount = flat_order_sum.head(10)
        top10_order = flat_order_count.head(10)
        fig_sum = px.bar(top10_amount.sort_values(by ='amount', ascending = True), x= 'amount', y= 'seller_id', title="Based on Sales Amount", labels = {"amount": "Sales Amount (in Real Brazil)", "seller_id": "Seller ID"})
        fig_count = px.bar(top10_order.sort_values(by ='order_id', ascending = True), x= 'order_id', y= 'seller_id', title="Based on Number of Orders", labels = {"order_id": "Number of Orders", "seller_id": "Seller ID"})
        fig_sum.update_layout(font=dict(size=14))
        fig_count.update_layout(font=dict(size=14))
        
        flat_order_sum_2 = flat_order_sum.sort_values(by = 'amount', ascending = True) #sort by ascending value after making sure that the pivot table is not empty
        flat_order_count_2 = flat_order_count.sort_values(by = 'order_id', ascending = True) #sort by ascending value after making sure that the pivot table is not empty
        lowest10_amount = flat_order_sum_2.head(10)
        lowest10_order = flat_order_count_2.head(10)
        fig_sum_2 = px.bar(lowest10_amount.sort_values(by ='amount', ascending = False), x= 'amount', y= 'seller_id', title="Based on Sales Amount", labels = {"amount": "Sales Amount (in Real Brazil)", "seller_id": "Seller ID"})
        fig_count_2 = px.bar(lowest10_order.sort_values(by ='order_id', ascending = False), x= 'order_id', y= 'seller_id', title="Based on Number of Orders", labels = {"order_id": "Number of Orders", "seller_id": "Seller ID"})
        fig_sum_2.update_layout(font=dict(size=14))
        fig_count_2.update_layout(font=dict(size=14))
        
        fig_trend_amount = px.line(filtered_new_seller_growth_amount,
                                   x = "transaction_age_mark",
                                   y = "amount",
                                   color = "seller_id",
                                   title = "Trend of Sales Amount",
                                   labels = {"amount": "Sales Amount (in Real Brazil)", "transaction_age_mark": "Month Age of Seller"},
                                   markers = True)
        
        fig_trend_count = px.line(filtered_new_seller_growth_count,
                                  x = "transaction_age_mark",
                                  y = "order_id",
                                  color = "seller_id",
                                  title = "Trend of Number of Orders",
                                  labels = {"order_id": "Number of Orders", "transaction_age_mark": "Month Age of Seller"},
                                  markers = True)
        return fig_sum, fig_count, fig_sum_2, fig_count_2,fig_trend_amount, fig_trend_count
    

# Run the application
if __name__ == "__main__":
    app.run(debug=True)