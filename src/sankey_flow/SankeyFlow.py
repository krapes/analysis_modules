import pandas as pd
import datetime
import time

import plotly.graph_objects as go
import chart_studio

chart_studio.tools.set_credentials_file(username='kerri_jaya', api_key='pMY9gMxFv3QNRFBSfiV1')


class SankeyFlow:
    # Create a set of colors that you'd like to use in your plot.
    default_palette = ['50BE97', 'E4655C', 'FCC865',
                       'BFD6DE', '3E5066', '353A3E', 'E6E6E6']
    title = None
    path_highlight = '1-Path_Freq_Rank'
    _threshold = 0

    def __init__(self, data: pd.DataFrame = None, palette: list = None) -> None:
        self._data = data

    @property
    def data(self) -> pd.DataFrame:
        """ Sequence data used for creating the plot
        """
        return self._data

    @data.setter
    def data(self, value: pd.DataFrame) -> None:
        self._data = value

    @property
    def threshold(self) -> pd.DataFrame:
        """ Sequence data used for creating the plot
        """
        return self._threshold

    @threshold.setter
    def threshold(self, value: int) -> None:
        if hasattr(self, 'sourceTargetDf') == False:
            self._threshold = value
        else:
            print(f"max links {self.sourceTargetDf['count'].max()}")
            self._threshold = int(self.sourceTargetDf['count'].max() * (value / 100))
        print(f"threshold set to {self._threshold} by parameter {value}")

    @staticmethod
    def _build_node_dict(data: pd.DataFrame, palette: list) -> dict:

        grouped = data.groupby('event_name')
        ideal_node_locations = grouped.rank_event.apply(lambda row: row.mode())
        label_list = ideal_node_locations.sort_values(by=['rank_event']).event_name.to_list()
        return label_list

    def build_sourceTargetDf(self,
                             df,
                             cat_cols=['event_name', 'next_event'],
                             value_cols=['count', 'time_from_start'],
                             color_col=['color']):
        '''
        if len(df) > 10000:
            print("Sampling Dataset")
            df = df.sample(10000)
        '''
        # maximum of 6 value cols -> 6 colors
        colorPalette = ['#4B8BBE', '#306998', '#FFE873', '#FFD43B', '#646464']
        labelList = []
        colorNumList = []
        '''
        for catCol in cat_cols:
            #labelListTemp = list(set(df[catCol].values))
            colorNumList.append(len(labelListTemp))
            #labelList = labelList + labelListTemp

        # remove duplicates from labelList
        #labelList = list(dict.fromkeys(labelList))
        '''
        grouped = df.groupby('event_name')
        ideal_node_locations = pd.DataFrame(grouped.rank_event.apply(lambda row: row.mode()))
        ideal_node_locations.reset_index(inplace=True)
        labelList = ideal_node_locations.sort_values('rank_event').event_name.to_list()


        # define colors based on number of levels
        # colorList = []
        #for idx, colorNum in enumerate(colorNumList):
        #    colorList = colorList + [colorPalette[idx]] * colorNum
        colorList = [colorPalette[0]]*len(labelList)

        # transform df into a source-target pair
        for i in range(len(cat_cols) - 1):
            if i == 0:
                sourceTargetDf = df[cat_cols + value_cols + color_col]
                sourceTargetDf.columns = ['source', 'target'] + value_cols + color_col
            else:
                tempDf = df[[cat_cols[i], cat_cols[i + 1], value_cols]]
                tempDf.columns = ['source', 'target', 'count']
                sourceTargetDf = pd.concat([sourceTargetDf, tempDf])
            sourceTargetDf = (sourceTargetDf
                              .groupby(['source', 'target'] + color_col)
                              .agg({'count': 'sum', 'time_from_start': 'mean'})
                              .reset_index())

        # add index for source-target pair
        sourceTargetDf['sourceID'] = sourceTargetDf['source'].apply(lambda x: labelList.index(x))
        sourceTargetDf['targetID'] = sourceTargetDf['target'].apply(lambda x: labelList.index(x))

        return labelList, colorList, sourceTargetDf

    def genSankey(self,
                  sourceTargetDf,
                  labelList,
                  colorList,
                  colored_path,
                  threshold=0,
                  title='Sankey Diagram'):
        print(f"plotting parameters {threshold}   {colored_path}")
        sourceTargetDf['color'] = '#d3d3d3'

        if colored_path is not None:
            sourceTargetDf.loc[sourceTargetDf['path_nickname'] == colored_path, 'color'] = '#ed7953'
        else:
            print("Colored path is none")
        sourceTargetDf.loc[sourceTargetDf['callback_instance'] == 1, 'color'] = '#800000'
        sourceTargetDf = sourceTargetDf[sourceTargetDf['count'] >= threshold]
        # creating the sankey diagram
        data = dict(
            type='sankey',
            node=dict(
                pad=15,
                thickness=20,
                line=dict(
                    color="black",
                    width=0.5
                ),
                label=labelList,
                color=colorList
            ),
            link=dict(
                source=sourceTargetDf['sourceID'],
                target=sourceTargetDf['targetID'],
                value=sourceTargetDf['count'],
                color=sourceTargetDf['color'],
                label=sourceTargetDf['time_from_start'],
                customdata=sourceTargetDf['path_nickname'],
                hovertemplate='%{value} unique users went from %{source.label} to %{target.label}.<br />' +
                              'on path %{customdata} ' +
                              '<br />It took them %{label} seconds on average from the start of the flow to finish ' +
                              'event %{target.label}.<extra></extra>'
            )
        )

        layout = dict(
            title=title,
            font=dict(
                size=10
            )
        )

        fig = go.Figure(dict(data=[data], layout=layout))
        return fig

    def plot(self,
             threshold: int,
             title: str,
             start_date: datetime.date = None,
             end_date: datetime.date = None) -> go.Figure:

        if self._data is None or len(self._data) == 0:
            raise Exception("SanKeyFlow self._data cannot be None or len zero")

        self.title = title
        data = self._data.copy()
        print("Starting genSankey")
        start_time = time.time()
        self.labelList, self.colorList, self.sourceTargetDf = self.build_sourceTargetDf(data,
                                                                                        color_col=['path_nickname',
                                                                                                   'callback_instance'])
        fig = self.genSankey(
            self.sourceTargetDf,
            self.labelList,
            self.colorList,
            self.path_highlight,
            threshold=self._threshold,
            title=self.title)
        print(f"Finished in {round((time.time() - start_time) * 60, 2)}")

        return fig

    def sankey_modify_threshold(self, threshold: int) -> go.Figure:

        if self.title is None:
            raise Exception("Method 'plot' needs to be run before modify_threshold")
        self._threshold = threshold
        fig = self.genSankey(
            self.sourceTargetDf,
            self.labelList,
            self.colorList,
            self.path_highlight,
            threshold=threshold,
            title=self.title)

        return fig

    def sankey_modify_path_highlight(self, path_nickname: str) -> go.Figure:

        if self.title is None:
            raise Exception("Method 'plot' needs to be run before modify_threshold")
        self.path_highlight = path_nickname
        fig = self.genSankey(
            self.sourceTargetDf,
            self.labelList,
            self.colorList,
            self.path_highlight,
            threshold=self._threshold,
            title=self.title)

        return fig
