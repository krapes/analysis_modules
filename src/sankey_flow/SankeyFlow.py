
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
    path_highlight = None
    threshold = 0

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

    def build_sourceTargetDf(self,
                             df,
                             cat_cols=['event_name', 'next_event'],
                             value_cols=['count', 'time_from_start'],
                             color_col=['color']):
        # maximum of 6 value cols -> 6 colors
        colorPalette = ['#4B8BBE', '#306998', '#FFE873', '#FFD43B', '#646464']
        labelList = []
        colorNumList = []
        for catCol in cat_cols:
            labelListTemp = list(set(df[catCol].values))
            colorNumList.append(len(labelListTemp))
            labelList = labelList + labelListTemp

        # remove duplicates from labelList
        labelList = list(dict.fromkeys(labelList))

        # define colors based on number of levels
        colorList = []
        for idx, colorNum in enumerate(colorNumList):
            colorList = colorList + [colorPalette[idx]] * colorNum

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

        sourceTargetDf['color'] = '#808080'
        if colored_path is not None:
            sourceTargetDf.loc[sourceTargetDf['path_nickname'] == colored_path, 'color'] = '#ed7953'
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
                hovertemplate='%{value} unique users went from %{source.label} to %{target.label}.<br />' +
                              '<br />It took them %{label} on average from the start of the flow to finish ' +
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
                                                                                        color_col=['path_nickname'])
        fig = self.genSankey(
                  self.sourceTargetDf,
                  self.labelList,
                  self.colorList,
                  self.path_highlight,
                  threshold=self.threshold,
                  title=self.title)
        print(f"Finished in {round((time.time() - start_time)*60, 2)}")

        return fig

    def sankey_modify_threshold(self, threshold: int) -> go.Figure:

        if self.title is None:
            raise Exception("Method 'plot' needs to be run before modify_threshold")
        self.threshold = threshold
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
                            threshold=self.threshold,
                            title=self.title)

        return fig
