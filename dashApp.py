import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash
from sqlalchemy import create_engine
import pandas as pd
import plotly.graph_objs as go
import dash_table_experiments as dt
import plotly.plotly as plt

app = dash.Dash()

md = '''
## Dash and Markdown

Dash apps can be written in Markdown.

*Italic*
**Bold**
# H1
## H2
* List 1
1. List 2
Horizontal Rule
---
'''
# # Create sqlalchemy engine to connect to the database
engine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\Monitors\CIXS\DashRepository\database.db')
CIXSengine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\Monitors\CIXS\DashRepository\CIXS.db')

# Read sql table into a dataframe
secMaster = pd.read_sql_table('secMaster', con=engine)
secMasterShort = secMaster[secMaster['Shortable'] == "Y"]
table = pd.read_sql_table('GACGB1', con=engine)


def generate_table(dataframe, max_rows=10000):
    '''
    This function returns a html table for a given passed dataframe
    :param dataframe:
    :param max_rows:
    :return:
    '''
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )


def generate_CIXS_df(tableName):
    df = pd.read_sql_table(tableName, con=CIXSengine)
    print(df.head())
    return df

def generate_Sec_df(tableName):
    df = pd.read_sql_table(tableName, con=engine)
    return df


veto = [
    'YellowKey',
    'SecIDType',
    'Tenor_Num',
    'Tenor_Unit'
]


def generate_dropdown_dict(series):
    list = []
    for x in series:
        d = dict(label=x, value=x)
        list.append(d)
    print(list[0:5])
    return list


'''
So everything we want to show goes into the once html.Div(children=[])
-- Within this, you can declare all of the components that you want to show
'''
app.config['suppress_callback_exceptions']=True

app.layout = html.Div([
    # html.H1(children='Hello Dash'),
    #
    # html.Div(children='''
    #     Dash: A web application framework for Python.
    # '''),

    dcc.Markdown(md),
    # generate_table(secMaster),
    dt.DataTable(
        rows=secMaster.to_dict('records'),

        # optional - sets the order of columns
        columns=[x for x in secMaster.columns if x not in veto],
        # columns=sorted(secMaster.columns),

        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='tester'
    ),
    html.Div([
        html.Div([
            dcc.Dropdown(
                # options=[generate_dropdown_dict(secMaster['SecID'])],
                multi=False,
                id='drop1',
                value='GACGB1',
                options = [{'label': secID, 'value': secID} for secID in secMasterShort['SecID']]
            )
        ], className="three columns"),
        html.Div([
            dcc.Dropdown(
                # options=[generate_dropdown_dict(secMaster['SecID'])],
                multi=False,
                id='drop2',
                value='GACGB1',
                options=[{'label': secID, 'value': secID} for secID in secMaster['SecID']]
            )
        ], className="three columns"),
        html.Div([
            dcc.RadioItems(
                options=[
                    {'label': '1 Month', 'value': '1'},
                    {'label': '3 Month', 'value': '3'},
                    {'label': '6 Month', 'value': '6'},
                    {'label': '1 Year', 'value': '12'},
                    {'label': '3 Year', 'value': '36'},
                    {'label': '5 Year', 'value': '60'},
                    {'label': 'Max', 'value': 'Max'}
                ],
                value='Max',
                id='radio',
                labelStyle={'display': 'inline-block'}
            )
        ], className="six columns"),
    ], className="row"),

    html.Div([
        html.Div([
            html.H3('Historical Yield'),
            dcc.Graph(id = 'yieldHist')
        ], className="eight columns"),
        html.Div([
            html.H3('Yield Distribution'),
            dcc.Graph(id = 'yieldDist')
        ], className="four columns"),
    ], className="row"),

    html.Div([
        html.Div([
            html.H3('Historical Spread'),
            dcc.Graph(id='spreadHist')
        ], className="eight columns"),
        html.Div([
            html.H3('Spread Distribution'),
            dcc.Graph(id='spreadDist')
        ], className="four columns"),
    ], className="row")
])

@app.callback(Output('yieldHist', 'figure'), [Input('drop1', 'value'), Input('drop2', 'value')])
def generate_yield_hist(val1, val2):
    print(val1 + " " + val2)
    secList = [val1, val2]
    traces = []
    for i in (secList):
        df = generate_Sec_df(i)
        traces.append(go.Scatter(
            x = df.date,
            y = df.PX_LAST,
            name = i
        ))
    return {
        'data' : traces,
        'layout' : go.Layout(
                    title='Test',
                    showlegend=True,
                    legend=go.Legend(x=0, y=1.0),
                    margin=go.Margin(
                        l=40,
                        r=0,
                        t=40,
                        b=30
                    ))
    }

@app.callback(Output('spreadHist', 'figure'), [Input('drop1', 'value'), Input('drop2', 'value')])
def generate_spread_hist(val1, val2):
    print(val1 + " " + val2)
    CIXS = val1 + "_" + val2
    print(CIXS)
    traces = []
    df = generate_CIXS_df(CIXS)
    traces.append(go.Scatter(
        x = df.date,
        y = df.Spread,
        name = CIXS
    ))
    return {
        'data' : traces,
        'layout' : go.Layout(
                    title='Test',
                    showlegend=True,
                    legend=go.Legend(x=0, y=1.0),
                    margin=go.Margin(
                        l=40,
                        r=0,
                        t=40,
                        b=30
                    ))
    }

@app.callback(Output('yieldDist', 'figure'), [Input('drop1', 'value')])
def generate_yield_dist(val1):
    traces = []
    df = generate_Sec_df(val1)
    traces.append(go.Histogram(
        y = df.PX_LAST,
        histnorm = 'probability',
        opacity = .65,
        name = val1
    ))
    return {
        'data' : traces,
        'layout' : go.Layout(
                    title='Test',
                    showlegend=True,
                    barmode = "overlay",
                    legend=go.Legend(x=0, y=1.0),
                    margin=go.Margin(
                        l=40,
                        r=0,
                        t=40,
                        b=30
                    ))
    }

@app.callback(Output('spreadDist', 'figure'), [Input('drop1', 'value'), Input('drop2', 'value')])
def generate_spread_dist(val1, val2):
    print(val1 + " " + val2)
    CIXS = val1 + "_" + val2
    print(CIXS)
    traces = []
    df = generate_CIXS_df(CIXS)
    traces.append(go.Histogram(
        y = df.Spread,
        histnorm = 'probability',
        opacity = .65,
        name = CIXS
    ))
    return {
        'data' : traces,
        'layout' : go.Layout(
                    title='Test',
                    showlegend=True,
                    barmode = "overlay",
                    legend=go.Legend(x=0, y=1.0),
                    margin=go.Margin(
                        l=40,
                        r=0,
                        t=40,
                        b=30
                    ))
    }


app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})



if __name__ == '__main__':
    app.run_server()
