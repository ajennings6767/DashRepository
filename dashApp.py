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
engine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\Data\database.db')
CIXSengine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\Data\CIXS.db')

# Read sql table into a dataframe
secMaster = pd.read_sql_table('secMaster', con=engine)
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

    dcc.Dropdown(
        # options=[generate_dropdown_dict(secMaster['SecID'])],
        multi=True,
        id='drop',
        value='GACGB1',
        options = [{'label': secID, 'value': secID} for secID in secMaster['SecID']]
    ),
    dcc.RadioItems(
        options = [
            {'label': '1 Month', 'value': '1'},
            {'label': '3 Month', 'value': '3'},
            {'label': '6 Month', 'value': '6'},
            {'label': '1 Year', 'value': '12'},
            {'label': '3 Year', 'value': '36'},
            {'label': '5 Year', 'value': '60'},
            {'label': 'Max', 'value': 'Max'}
        ],
        value = 'Max',
        labelStyle = {'display': 'inline-block'}
    ),
    html.Div([
        html.Div([
            html.H3('Historical Yield'),
            dcc.Graph(id = 'graph2')
        ], className="eight columns"),
        html.Div([
            html.H3('Yield Distribution'),
            dcc.Graph(id = 'testGraph')
        ], className="four columns"),
    ], className="row"),

    html.Div([
        html.Div([
            html.H3('Historical Spread'),
            dcc.Graph(id='graph3')
        ], className="twelve columns"),
    ], className="row")
])

@app.callback(Output('graph2', 'figure'), [Input('drop', 'value')])
def generate_graph(secList):
    print(secList)
    traces = []
    for i in (secList):
        df = generate_CIXS_df(i)
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

@app.callback(Output('graph3', 'figure'), [Input('drop', 'value')])
def generate_graph(secList):
    print(secList)
    traces = []
    for i in (secList):
        df = generate_CIXS_df(i)
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

@app.callback(Output('testGraph', 'figure'), [Input('drop', 'value')])
def generate_graph(secList):
    print(secList)
    traces = []
    for i in secList:
        df = generate_CIXS_df(i)
        traces.append(go.Histogram(
            y = df.PX_LAST,
            histnorm = 'probability',
            opacity = .65,
            name = i
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
