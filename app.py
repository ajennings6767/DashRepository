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

md ='''
## Dash and Markdown

Dash apps can be written in Markdown.
Dash uses the [CommonMark](http://commonmark.org/)
specification of Markdown.
Check out their [60 Second Markdown Tutorial](http://commonmark.org/help/)
if this is your first introduction to Markdown!

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
    html.Div([
        html.Div([
            html.H3('Column 1'),
            dcc.Graph(
                id = 'testGraph',
                figure = go.Figure(
                    data=[
                        go.Scatter(
                            x=table.date,
                            y=table['PX_LAST'],
                            name='ACGB2',
                            line=dict(color='#17BECF'),
                            opacity=.8
                        )
                    ],
                    layout=go.Layout(
                        title='Test',
                        showlegend=True,
                        legend=go.Legend(x=0, y=1.0),
                        margin = go.Margin(
                            l=40,
                            r=0,
                            t=40,
                            b=30
                        )
                    )
                )
            )
        ], className="six columns"),
        html.Div([
            html.H3('Column 2'),
            dcc.Graph(id = 'graph2')
        ], className="six columns"),
    ], className="row")
])

@app.callback(Output('graph2', 'figure'), [Input('drop', 'value')])
def generate_graph(secList):
    print(secList)
    return {
        'data' : go.Scatter(
                    x=table.date,
                    y=table['PX_LAST'],
                    name='ACGB2',
                    line=dict(color='#17BECF'),
                    opacity=.8
                    ),
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

app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})



if __name__ == '__main__':
    app.run_server()