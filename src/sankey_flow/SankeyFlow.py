import seaborn as sns
import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy import stats
import logging
import copy

import plotly.graph_objects as go
import chart_studio
import chart_studio.plotly as py
import plotly

chart_studio.tools.set_credentials_file(username='kerri_jaya', api_key='pMY9gMxFv3QNRFBSfiV1')



class SankeyFlow:

	# Create a set of colors that you'd like to use in your plot.
	default_palette = ['50BE97', 'E4655C', 'FCC865',
			   'BFD6DE', '3E5066', '353A3E', 'E6E6E6']
	title = None

	def __init__(self, data: pd.DataFrame, palette: list = None):
		if len(data) == 0:
			raise Exception("len(data) cannot be zero")
		self._data = data
		self._palette = (palette if palette != None
							else self._get_palette(self.default_palette,
												(data.event_name.unique())))


	@staticmethod
	def _convert_datetimes(data: pd.DataFrame) -> pd.DataFrame:
		# Making sure that time_event and time_install are Pandas Datetime types:
		data['time_event'] = pd.to_datetime(data['time_event'], unit='s')
		return data

	@staticmethod
	def _format_df(data: pd.DataFrame) -> pd.DataFrame:
		# Based on the time of events, we can compute the rank of each action at
		# the user_id level:

		# a) Sort ascendingly per user_id and time_event
		# sort by event_type to make sure installs come first
		data.sort_values(['user_id', 'time_event'],
						 ascending=[True, True], inplace=True)

		# b) Group by user_id
		grouped = data.groupby('user_id')
		# c) Define a ranking function based on time_event, using the method = 'first'
		# param to ensure no events have the same rank
		def rank(x): return x['time_event'].rank(method='first').astype(int)
		# d) Apply the ranking function to the data DF into a new "rank_event" column
		data["rank_event"] = grouped.apply(rank).reset_index(0, drop=True)


		# Add, each row, the information about the next_event
		# a) Regroup by user_id
		grouped = data.groupby('user_id')
		# b) The shift function allows to access the next row's data. Here, we'll want the event name
		def get_next_event(x): return x['event_name'].shift(-1)
		# c) Apply the function into a new "next_event" column
		data["next_event"] = grouped.apply(
			lambda x: get_next_event(x)).reset_index(0, drop=True)


		# Likewise, we can compute time from first event to its this event:
		# a) Regroup by user_id
		grouped = data.groupby('user_id')
		# b) We make use one more time of the shift function:
		def get_time_diff(
			x): return  x['time_event'] - x['time_event'].min() #x['time_event'].shift(-1) - x['time_event']
		# c) Apply the function to the data DF into a new "time_to_next" column
		data["time_from_start"] = grouped.apply(
			lambda x: get_time_diff(x)).reset_index(0, drop=True)

		# Likewise, we can compute time from each event to its next event:
		# a) Regroup by user_id
		grouped = data.groupby('user_id')
		# b) We make use one more time of the shift function:
		def get_time_diff(
			x): return  x['time_event'].shift(-1) - x['time_event']
		# c) Apply the function to the data DF into a new "time_to_next" column
		data["time_to_next"] = grouped.apply(
			lambda x: get_time_diff(x)).reset_index(0, drop=True)

		return data

	
	def _get_palette(self, palette: list, all_events: list) -> list:


		#  Here, I passed the colors as HEX, but we need to pass it as RGB. This loop
		# will convert from HEX to RGB:
		for i, col in enumerate(palette):
			for i in (0, 2, 4):
				try:
					palette[i] = tuple(int(col[i:i+2], 16))
				except:
					pass

		# Append a Seaborn complementary palette to your palette in case you did not
		# provide enough colors to style every event
		complementary_palette = sns.color_palette(
			"deep", len(all_events) - len(palette))
		if len(complementary_palette) > 0:
			palette.extend(complementary_palette)

		return palette


	@staticmethod
	def _build_node_dict(data: pd.DataFrame, palette: list) -> dict:

		
		grouped = data.groupby('event_name')
		ideal_node_locations = grouped.rank_event.apply(lambda row: row.mode())
		nodes = pd.DataFrame(ideal_node_locations).groupby('rank_event')

		i = 0
		nodes_dict = {}
		for node in nodes:

			all_events_at_this_rank = [e[0] for e in node[1].index.to_list()]

			# Read the colors for these events and store them in a list...
			#rank_palette = []
			#for event in all_events_at_this_rank:
			#	rank_palette.append(palette[list(all_events).index(event)])

				# Create a new key equal to the rank...
			nodes_dict.update(
				{node[0]: dict()}
			)

			nodes_dict[node[0]].update(
				{
					'sources': all_events_at_this_rank,
					#'color': rank_palette,
					'sources_index': list(range(i, i+len(all_events_at_this_rank)))
				}
			)

			# Finally, increment by the length of this rank's available events to make
			# sure next indices will not be chosen from existing ones
			i += len(nodes_dict[node[0]]['sources_index'])

		return nodes_dict

	def _find_rank(self, target, event_name, nodes_dict):
		rank = -1
		try_rank = 0
		nodes = [n for n in nodes_dict.keys() if type(n) == int]
		while rank < 0 and rank < max(nodes):
			try_rank += 1
			if event_name in nodes_dict[try_rank][target]:
				rank = try_rank
		if rank < 0: raise Exception(f"{event_name} not found in nodes_dict \n {nodes_dict}")
		return rank



	def _build_link_dict(self, data: pd.DataFrame, nodes_dict: dict) -> dict:
		output = dict()
		output.update({'links_dict': dict()})

		# Group the DataFrame by user_id and rank_event
		grouped = data.groupby(['user_id', 'event_name', 'rank_event', 'next_event'])

		# Define a function to read the souces, targets, values and time from event to next_event:
		def update_source_target(user):
			try:
				# user.name[0] is the user's user_id; user.name[1] is the rank of each action
				# 1st we retrieve the source and target's indices from nodes_dict

				node_source = self._find_rank('sources', user.name[1], nodes_dict)
				node_target = self._find_rank('sources', user.name[3], nodes_dict)
				source_index = nodes_dict[node_source]['sources_index'][nodes_dict
																			[node_source]['sources'].index(user['event_name'].values[0])]
				target_index = nodes_dict[node_target]['sources_index'][nodes_dict
																			[node_target]['sources'].index(user['next_event'].values[0])]

				 # If this source is already in links_dict...
				if source_index in output['links_dict']:
					# ...and if this target is already associated to this source...
					if target_index in output['links_dict'][source_index]:
						# ...then we increment the count of users with this source/target pair by 1, and keep track of the time from source to target
						output['links_dict'][source_index][target_index]['unique_users'] += 1
						output['links_dict'][source_index][target_index]['avg_time_from_start'] += user["time_from_start"].values[0]
					# ...but if the target is not already associated to this source...
					else:
						# ...we create a new key for this target, for this source, and initiate it with 1 user and the time from source to target
						output['links_dict'][source_index].update({target_index:
																   dict(
																	   {'unique_users': 1,
																		'avg_time_from_start': user["time_from_start"].values[0]}
																   )
																   })
				# ...but if this source isn't already available in the links_dict, we create its key and the key of this source's target, and we initiate it with 1 user and the time from source to target
				else:
					output['links_dict'].update({source_index: dict({target_index: dict(
						{'unique_users': 1, 'avg_time_from_start': user["time_from_start"].values[0]})})})
			except Exception as e:
				#pass1
				raise e

		# Apply the function to your grouped Pandas object:
		grouped.apply(lambda user: update_source_target(user))

		return output['links_dict']



	def trim_links(self, threshold: int = 300) -> dict:


		def rev_eng_event_name(i):
			idx = nodes_dict[self._find_rank('sources_index', i, nodes_dict)]['sources_index'].index(i)
			source_event = nodes_dict[self._find_rank('sources_index', i, nodes_dict)]['sources'][idx]
			return source_event

		links_dict = copy.deepcopy(self.links_dict)
		nodes_dict = self.nodes_dict
		level_1s = links_dict.keys()

		for level_1 in level_1s:
			level_2s = list(links_dict[level_1].keys())
			for level_2 in level_2s:
				if links_dict[level_1][level_2]['unique_users'] < threshold:
					logging.debug(f"Source: {rev_eng_event_name(level_1):35} Target: {rev_eng_event_name(level_2):35}" +
							f"   unique_users: {links_dict[level_1][level_2]['unique_users']}")
					del links_dict[level_1][level_2]

		return links_dict


	def _seperate_lists(self, links_dict: dict, nodes_dict: dict) -> dict:
		targets = []
		sources = []
		values = []
		time_from_start = []

		for source_key, source_value in links_dict.items():
			for target_key, target_value in links_dict[source_key].items():
				sources.append(source_key)
				targets.append(target_key)
				values.append(target_value['unique_users'])
				time_from_start.append(str(pd.to_timedelta(
					target_value['avg_time_from_start'] / target_value['unique_users'])).split('.')[0])
					# Split to remove the milliseconds information

		labels = []
		colors = []
		for key, value in nodes_dict.items():
			labels = labels + list(nodes_dict[key]['sources'])
			#colors = colors + list(nodes_dict[key]['color'])

		#for idx, color in enumerate(colors):
		#	colors[idx] = "rgb" + str(color) + ""


		return {'targets':targets,
				'sources': sources,
				'values': values,
				'time_from_start': time_from_start,
				'labels': labels}
				#'colors': colors}


	def _create_figure(self, output: dict, title: str):

		plotting_features = self._seperate_lists(output['links_dict'],
											output['nodes_dict'])

		
		fig = go.Figure(data=[go.Sankey(
		node=dict(
			thickness=10,  # default is 20
			line=dict(color="black", width=0.5),
			label=plotting_features['labels'],
			#color=plotting_features['colors']
		),
		link=dict(
			source=plotting_features['sources'],
			target=plotting_features['targets'],
			value=plotting_features['values'],
			label=plotting_features["time_from_start"],
			hovertemplate='%{value} unique users went from %{source.label} to %{target.label}.<br />' +
			'<br />It took them %{label} in average to reach this point from the start of the flow.<extra></extra>',
		))])

		fig.update_layout(autosize=True,
						  #width=2000,
						  #height=2000,
						  title_text=title,
						  font=dict(size=15),
						  plot_bgcolor='white')
	
		return fig

	def plot(self, threshold: int, title: str):

		self.title = title
		palette = self._palette
		data = self._data
		data = self._convert_datetimes(data)
		data = self._format_df(data)

		nodes_dict = self._build_node_dict(data, palette)
		links_dict = self._build_link_dict(data, nodes_dict)
		self.nodes_dict = nodes_dict
		self.links_dict = links_dict

		output = dict()
		output['nodes_dict'] = nodes_dict
		output['links_dict'] = self.trim_links(threshold)

		fig = self._create_figure(output, title)

		return fig


	def modify_threshold(self, threshold: int):

		if self.title is None:
			raise Exception("Method 'plot' needs to be run before modify_threshold")

		output = dict()
		output['nodes_dict'] = self.nodes_dict
		output['links_dict'] = self.trim_links(threshold)

		fig = self._create_figure(output, self.title)

		return fig



