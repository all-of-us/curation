# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
# ---

from notebooks import parameters

# +
#######################################
print('Setting everything up...')
#######################################

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from matplotlib.lines import Line2D

import matplotlib.ticker as ticker
import matplotlib.cm as cm
import matplotlib as mpl

import matplotlib.pyplot as plt
# %matplotlib inline
import seaborn as sns

import os
import sys
from datetime import datetime
from datetime import date
from datetime import time
from datetime import timedelta
import time

import seaborn as sns
plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999

from IPython.display import HTML as html_print

dataset_id = parameters.UNION_DATASET_ID

def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


print('done.')
# -

# # b) How many rows does this affect?

# +
# Number of rows affected

number_of_rows_affected = '''

select distinct count (*) as number_of_rows_affected from {DATASET_ID}.procedure_occurrence
where procedure_source_concept_id in  (42628107, 42628103, 42628379, 4320851, 4320730, 4320556, 4320557, 4320729, 4320558, 42628410, 42628290, 42628380, 42628118, 42628329, 42628330, 42628111, 42628285, 42628101, 2007440, 45888282, 45888253, 42628263, 42628175, 42628176, 42628318, 42739578, 42628371, 42628204, 42628313, 42628403, 42739595, 42739597, 42628177, 42628178, 42628319, 42628412, 42628413, 42628280, 42628429, 42628168, 42628398, 42628119, 42628435, 42628184, 42739579, 42628320, 42628110, 42628384, 42628126, 42628315, 42628406, 42628191, 42628151, 42628116, 42628117, 42628167, 42628414, 926912, 926913, 42628247, 42628440, 42628261, 42739586, 724952, 724951, 724950, 42628297, 42628409, 45888264, 42628123, 926916, 42739676, 42739675, 42739582, 42739588, 42628389, 42628434, 42628185, 42628121, 42628390, 42628255, 42628188, 42628250, 42628291, 42628157, 42628156, 42628328, 42628358, 42628279, 42628370, 42628236, 42628138, 42628099, 42628268, 42628098, 42628405, 42628137, 42628404, 42628183, 926908, 926910, 42628155, 42628262, 42628396, 42628324, 42628273, 42628286, 926914, 926915, 42628271, 42628321, 42628272, 42628419, 42628190, 42628274, 42628362, 42628147, 42628430, 42628257, 42628251, 42628312, 42628400, 42628170, 42628431, 42628115, 42628249, 42628248, 42628288, 42628114, 42628392, 42628253, 42628287, 724947, 42628129, 42628322, 42628323, 42628304, 42739572, 42628309, 42628310, 42628182, 42628113, 42628198, 42628146, 42628124, 42628332, 42628241, 42628411, 42628374, 42628316, 42628102, 42628425, 42628375, 42628104, 42628424, 42628395, 42628382, 42628293, 42628256, 45888252, 45888279, 45888280, 45888262, 45888275, 45888285, 45888266, 45888276, 45888251, 45888283, 45888258, 45888278, 45888265, 45888270, 42628428, 42628192, 42628152, 42628331, 42628289, 42628153, 42628433, 45888255, 45888257, 42628166, 42739577, 42628386, 42628265, 42628133, 42628365, 42628202, 42628366, 42628131, 42628266, 42628364, 42628447, 42628200, 42628132, 42628401, 42628201, 42628302, 42628194, 42628237, 42628363, 42628393, 42628421, 926911, 42628294, 42628443, 42628145, 926917, 42739596, 42628242, 42628180, 42628439, 42628418, 42628105, 42628179, 42628325, 42628417, 42628387, 42739599, 42740179, 42628277, 42739580, 42628122, 42628436, 42628158, 42628295, 42628144, 42628408, 724953, 42628326, 42628246, 42628449, 42628303, 42628196, 42628264, 42628160, 42628356, 42628327, 42628381, 42628376, 926909, 42628193, 42628416, 42628164, 45888284, 2102799, 2106116, 2108523, 2213392, 42628307, 724949, 42628306, 42628373, 42628240, 42628269, 42628239, 42628238, 42628142, 42628333, 42628335, 42628334, 42739584, 42628276, 42628426, 42739585, 42628162, 42628163, 42628300, 45888263, 42628314, 42628139, 42628141, 42628140, 42628445, 42740440, 42739576, 42628130, 42628391, 42628437, 45888261, 42628108, 42628260, 42628301, 42739581, 42739598, 42628100, 724987, 42628305, 42628127, 42628252, 42739976, 42739592, 42739591, 42628195, 42628441, 42628186, 42628292, 42628154, 42628254, 45888254, 45888274, 45888273, 45888286, 45888267, 45888272, 45888260, 45888259, 45888268, 45888269, 45888277, 45888271, 42628308, 42628296, 42628397, 42628399, 42628360, 42628165, 42628120, 42628361, 42628367, 42628173, 42628368, 42628298, 42628125, 42628438, 42628181, 42628378, 42628275, 42628415, 42628243, 42628244, 42628278, 42628448, 42628128, 42628442, 42628267, 42628134, 42628203, 42628135, 42628357, 42739575, 42628161, 42628187, 42628258, 42628171, 42628388, 42628432, 42628174, 42628199, 42628311, 42628427, 42628372, 42739587, 42628359, 42628444, 42628143, 42628112, 42739583, 42628259, 42628159, 42739590, 42628097, 42628446, 42628270, 42628317, 42628377, 42628394, 42628197, 42628385, 42628420, 42628423, 42628169, 42628299, 42628136, 42739589, 42628106, 42739593, 42739574, 42739594, 42739573, 42628369, 45888281, 45888256, 42628402, 42628172, 42628422, 42628109, 42628383, 42628245, 42628189, 926907, 724954)


'''.format(DATASET_ID=dataset_id)
number_of_rows_affected = pd.read_gbq(number_of_rows_affected,dialect="standard")
# -

number_of_rows_affected

# # c) LIst all PIDs this affects

# +
# List of PIDs that have been affected

pids_affected = '''

select distinct person_id as pids_affected from {DATASET_ID}.procedure_occurrence
where procedure_source_concept_id in (42628107, 42628103, 42628379, 4320851, 4320730, 4320556, 4320557, 4320729, 4320558, 42628410, 42628290, 42628380, 42628118, 42628329, 42628330, 42628111, 42628285, 42628101, 2007440, 45888282, 45888253, 42628263, 42628175, 42628176, 42628318, 42739578, 42628371, 42628204, 42628313, 42628403, 42739595, 42739597, 42628177, 42628178, 42628319, 42628412, 42628413, 42628280, 42628429, 42628168, 42628398, 42628119, 42628435, 42628184, 42739579, 42628320, 42628110, 42628384, 42628126, 42628315, 42628406, 42628191, 42628151, 42628116, 42628117, 42628167, 42628414, 926912, 926913, 42628247, 42628440, 42628261, 42739586, 724952, 724951, 724950, 42628297, 42628409, 45888264, 42628123, 926916, 42739676, 42739675, 42739582, 42739588, 42628389, 42628434, 42628185, 42628121, 42628390, 42628255, 42628188, 42628250, 42628291, 42628157, 42628156, 42628328, 42628358, 42628279, 42628370, 42628236, 42628138, 42628099, 42628268, 42628098, 42628405, 42628137, 42628404, 42628183, 926908, 926910, 42628155, 42628262, 42628396, 42628324, 42628273, 42628286, 926914, 926915, 42628271, 42628321, 42628272, 42628419, 42628190, 42628274, 42628362, 42628147, 42628430, 42628257, 42628251, 42628312, 42628400, 42628170, 42628431, 42628115, 42628249, 42628248, 42628288, 42628114, 42628392, 42628253, 42628287, 724947, 42628129, 42628322, 42628323, 42628304, 42739572, 42628309, 42628310, 42628182, 42628113, 42628198, 42628146, 42628124, 42628332, 42628241, 42628411, 42628374, 42628316, 42628102, 42628425, 42628375, 42628104, 42628424, 42628395, 42628382, 42628293, 42628256, 45888252, 45888279, 45888280, 45888262, 45888275, 45888285, 45888266, 45888276, 45888251, 45888283, 45888258, 45888278, 45888265, 45888270, 42628428, 42628192, 42628152, 42628331, 42628289, 42628153, 42628433, 45888255, 45888257, 42628166, 42739577, 42628386, 42628265, 42628133, 42628365, 42628202, 42628366, 42628131, 42628266, 42628364, 42628447, 42628200, 42628132, 42628401, 42628201, 42628302, 42628194, 42628237, 42628363, 42628393, 42628421, 926911, 42628294, 42628443, 42628145, 926917, 42739596, 42628242, 42628180, 42628439, 42628418, 42628105, 42628179, 42628325, 42628417, 42628387, 42739599, 42740179, 42628277, 42739580, 42628122, 42628436, 42628158, 42628295, 42628144, 42628408, 724953, 42628326, 42628246, 42628449, 42628303, 42628196, 42628264, 42628160, 42628356, 42628327, 42628381, 42628376, 926909, 42628193, 42628416, 42628164, 45888284, 2102799, 2106116, 2108523, 2213392, 42628307, 724949, 42628306, 42628373, 42628240, 42628269, 42628239, 42628238, 42628142, 42628333, 42628335, 42628334, 42739584, 42628276, 42628426, 42739585, 42628162, 42628163, 42628300, 45888263, 42628314, 42628139, 42628141, 42628140, 42628445, 42740440, 42739576, 42628130, 42628391, 42628437, 45888261, 42628108, 42628260, 42628301, 42739581, 42739598, 42628100, 724987, 42628305, 42628127, 42628252, 42739976, 42739592, 42739591, 42628195, 42628441, 42628186, 42628292, 42628154, 42628254, 45888254, 45888274, 45888273, 45888286, 45888267, 45888272, 45888260, 45888259, 45888268, 45888269, 45888277, 45888271, 42628308, 42628296, 42628397, 42628399, 42628360, 42628165, 42628120, 42628361, 42628367, 42628173, 42628368, 42628298, 42628125, 42628438, 42628181, 42628378, 42628275, 42628415, 42628243, 42628244, 42628278, 42628448, 42628128, 42628442, 42628267, 42628134, 42628203, 42628135, 42628357, 42739575, 42628161, 42628187, 42628258, 42628171, 42628388, 42628432, 42628174, 42628199, 42628311, 42628427, 42628372, 42739587, 42628359, 42628444, 42628143, 42628112, 42739583, 42628259, 42628159, 42739590, 42628097, 42628446, 42628270, 42628317, 42628377, 42628394, 42628197, 42628385, 42628420, 42628423, 42628169, 42628299, 42628136, 42739589, 42628106, 42739593, 42739574, 42739594, 42739573, 42628369, 45888281, 45888256, 42628402, 42628172, 42628422, 42628109, 42628383, 42628245, 42628189, 926907, 724954)


'''.format(DATASET_ID=dataset_id)
pids_affected = pd.read_gbq(pids_affected,dialect="standard")
# -

pids_affected.shape

pids_affected.to_csv('pids_affected.csv')

# # d) Is modifier_concept_id null for affected rows? If not, what is in modifier_concept_id?

# +
# Is modifier_concept_id null for affected rows? If not, what is in modifier_concept_id?

modifier_concept_id_null = '''

select distinct modifier_concept_id, COUNT(*) as count from {DATASET_ID}.procedure_occurrence
where procedure_source_concept_id in (42628107, 42628103, 42628379, 4320851, 4320730, 4320556, 4320557, 4320729, 4320558, 42628410, 42628290, 42628380, 42628118, 42628329, 42628330, 42628111, 42628285, 42628101, 2007440, 45888282, 45888253, 42628263, 42628175, 42628176, 42628318, 42739578, 42628371, 42628204, 42628313, 42628403, 42739595, 42739597, 42628177, 42628178, 42628319, 42628412, 42628413, 42628280, 42628429, 42628168, 42628398, 42628119, 42628435, 42628184, 42739579, 42628320, 42628110, 42628384, 42628126, 42628315, 42628406, 42628191, 42628151, 42628116, 42628117, 42628167, 42628414, 926912, 926913, 42628247, 42628440, 42628261, 42739586, 724952, 724951, 724950, 42628297, 42628409, 45888264, 42628123, 926916, 42739676, 42739675, 42739582, 42739588, 42628389, 42628434, 42628185, 42628121, 42628390, 42628255, 42628188, 42628250, 42628291, 42628157, 42628156, 42628328, 42628358, 42628279, 42628370, 42628236, 42628138, 42628099, 42628268, 42628098, 42628405, 42628137, 42628404, 42628183, 926908, 926910, 42628155, 42628262, 42628396, 42628324, 42628273, 42628286, 926914, 926915, 42628271, 42628321, 42628272, 42628419, 42628190, 42628274, 42628362, 42628147, 42628430, 42628257, 42628251, 42628312, 42628400, 42628170, 42628431, 42628115, 42628249, 42628248, 42628288, 42628114, 42628392, 42628253, 42628287, 724947, 42628129, 42628322, 42628323, 42628304, 42739572, 42628309, 42628310, 42628182, 42628113, 42628198, 42628146, 42628124, 42628332, 42628241, 42628411, 42628374, 42628316, 42628102, 42628425, 42628375, 42628104, 42628424, 42628395, 42628382, 42628293, 42628256, 45888252, 45888279, 45888280, 45888262, 45888275, 45888285, 45888266, 45888276, 45888251, 45888283, 45888258, 45888278, 45888265, 45888270, 42628428, 42628192, 42628152, 42628331, 42628289, 42628153, 42628433, 45888255, 45888257, 42628166, 42739577, 42628386, 42628265, 42628133, 42628365, 42628202, 42628366, 42628131, 42628266, 42628364, 42628447, 42628200, 42628132, 42628401, 42628201, 42628302, 42628194, 42628237, 42628363, 42628393, 42628421, 926911, 42628294, 42628443, 42628145, 926917, 42739596, 42628242, 42628180, 42628439, 42628418, 42628105, 42628179, 42628325, 42628417, 42628387, 42739599, 42740179, 42628277, 42739580, 42628122, 42628436, 42628158, 42628295, 42628144, 42628408, 724953, 42628326, 42628246, 42628449, 42628303, 42628196, 42628264, 42628160, 42628356, 42628327, 42628381, 42628376, 926909, 42628193, 42628416, 42628164, 45888284, 2102799, 2106116, 2108523, 2213392, 42628307, 724949, 42628306, 42628373, 42628240, 42628269, 42628239, 42628238, 42628142, 42628333, 42628335, 42628334, 42739584, 42628276, 42628426, 42739585, 42628162, 42628163, 42628300, 45888263, 42628314, 42628139, 42628141, 42628140, 42628445, 42740440, 42739576, 42628130, 42628391, 42628437, 45888261, 42628108, 42628260, 42628301, 42739581, 42739598, 42628100, 724987, 42628305, 42628127, 42628252, 42739976, 42739592, 42739591, 42628195, 42628441, 42628186, 42628292, 42628154, 42628254, 45888254, 45888274, 45888273, 45888286, 45888267, 45888272, 45888260, 45888259, 45888268, 45888269, 45888277, 45888271, 42628308, 42628296, 42628397, 42628399, 42628360, 42628165, 42628120, 42628361, 42628367, 42628173, 42628368, 42628298, 42628125, 42628438, 42628181, 42628378, 42628275, 42628415, 42628243, 42628244, 42628278, 42628448, 42628128, 42628442, 42628267, 42628134, 42628203, 42628135, 42628357, 42739575, 42628161, 42628187, 42628258, 42628171, 42628388, 42628432, 42628174, 42628199, 42628311, 42628427, 42628372, 42739587, 42628359, 42628444, 42628143, 42628112, 42739583, 42628259, 42628159, 42739590, 42628097, 42628446, 42628270, 42628317, 42628377, 42628394, 42628197, 42628385, 42628420, 42628423, 42628169, 42628299, 42628136, 42739589, 42628106, 42739593, 42739574, 42739594, 42739573, 42628369, 45888281, 45888256, 42628402, 42628172, 42628422, 42628109, 42628383, 42628245, 42628189, 926907, 724954)
GROUP BY modifier_concept_id

'''.format(DATASET_ID=dataset_id)
modifier_concept_id_null = pd.read_gbq(modifier_concept_id_null,dialect="standard")
# -

modifier_concept_id_null

# # e) What are the patterns? Does this affect certain individuals, certain types of procedures, etc.?

# +
# Patterns

concept = '''

select distinct person_id, procedure_source_concept_id, concept_id, concept_name, domain_id from {DATASET_ID}.procedure_occurrence as po
join {DATASET_ID}.concept c on (po.procedure_source_concept_id = c.concept_id)
where procedure_source_concept_id in (42628107, 42628103, 42628379, 4320851, 4320730, 4320556, 4320557, 4320729, 4320558, 42628410, 42628290, 42628380, 42628118, 42628329, 42628330, 42628111, 42628285, 42628101, 2007440, 45888282, 45888253, 42628263, 42628175, 42628176, 42628318, 42739578, 42628371, 42628204, 42628313, 42628403, 42739595, 42739597, 42628177, 42628178, 42628319, 42628412, 42628413, 42628280, 42628429, 42628168, 42628398, 42628119, 42628435, 42628184, 42739579, 42628320, 42628110, 42628384, 42628126, 42628315, 42628406, 42628191, 42628151, 42628116, 42628117, 42628167, 42628414, 926912, 926913, 42628247, 42628440, 42628261, 42739586, 724952, 724951, 724950, 42628297, 42628409, 45888264, 42628123, 926916, 42739676, 42739675, 42739582, 42739588, 42628389, 42628434, 42628185, 42628121, 42628390, 42628255, 42628188, 42628250, 42628291, 42628157, 42628156, 42628328, 42628358, 42628279, 42628370, 42628236, 42628138, 42628099, 42628268, 42628098, 42628405, 42628137, 42628404, 42628183, 926908, 926910, 42628155, 42628262, 42628396, 42628324, 42628273, 42628286, 926914, 926915, 42628271, 42628321, 42628272, 42628419, 42628190, 42628274, 42628362, 42628147, 42628430, 42628257, 42628251, 42628312, 42628400, 42628170, 42628431, 42628115, 42628249, 42628248, 42628288, 42628114, 42628392, 42628253, 42628287, 724947, 42628129, 42628322, 42628323, 42628304, 42739572, 42628309, 42628310, 42628182, 42628113, 42628198, 42628146, 42628124, 42628332, 42628241, 42628411, 42628374, 42628316, 42628102, 42628425, 42628375, 42628104, 42628424, 42628395, 42628382, 42628293, 42628256, 45888252, 45888279, 45888280, 45888262, 45888275, 45888285, 45888266, 45888276, 45888251, 45888283, 45888258, 45888278, 45888265, 45888270, 42628428, 42628192, 42628152, 42628331, 42628289, 42628153, 42628433, 45888255, 45888257, 42628166, 42739577, 42628386, 42628265, 42628133, 42628365, 42628202, 42628366, 42628131, 42628266, 42628364, 42628447, 42628200, 42628132, 42628401, 42628201, 42628302, 42628194, 42628237, 42628363, 42628393, 42628421, 926911, 42628294, 42628443, 42628145, 926917, 42739596, 42628242, 42628180, 42628439, 42628418, 42628105, 42628179, 42628325, 42628417, 42628387, 42739599, 42740179, 42628277, 42739580, 42628122, 42628436, 42628158, 42628295, 42628144, 42628408, 724953, 42628326, 42628246, 42628449, 42628303, 42628196, 42628264, 42628160, 42628356, 42628327, 42628381, 42628376, 926909, 42628193, 42628416, 42628164, 45888284, 2102799, 2106116, 2108523, 2213392, 42628307, 724949, 42628306, 42628373, 42628240, 42628269, 42628239, 42628238, 42628142, 42628333, 42628335, 42628334, 42739584, 42628276, 42628426, 42739585, 42628162, 42628163, 42628300, 45888263, 42628314, 42628139, 42628141, 42628140, 42628445, 42740440, 42739576, 42628130, 42628391, 42628437, 45888261, 42628108, 42628260, 42628301, 42739581, 42739598, 42628100, 724987, 42628305, 42628127, 42628252, 42739976, 42739592, 42739591, 42628195, 42628441, 42628186, 42628292, 42628154, 42628254, 45888254, 45888274, 45888273, 45888286, 45888267, 45888272, 45888260, 45888259, 45888268, 45888269, 45888277, 45888271, 42628308, 42628296, 42628397, 42628399, 42628360, 42628165, 42628120, 42628361, 42628367, 42628173, 42628368, 42628298, 42628125, 42628438, 42628181, 42628378, 42628275, 42628415, 42628243, 42628244, 42628278, 42628448, 42628128, 42628442, 42628267, 42628134, 42628203, 42628135, 42628357, 42739575, 42628161, 42628187, 42628258, 42628171, 42628388, 42628432, 42628174, 42628199, 42628311, 42628427, 42628372, 42739587, 42628359, 42628444, 42628143, 42628112, 42739583, 42628259, 42628159, 42739590, 42628097, 42628446, 42628270, 42628317, 42628377, 42628394, 42628197, 42628385, 42628420, 42628423, 42628169, 42628299, 42628136, 42739589, 42628106, 42739593, 42739574, 42739594, 42739573, 42628369, 45888281, 45888256, 42628402, 42628172, 42628422, 42628109, 42628383, 42628245, 42628189, 926907, 724954)

'''.format(DATASET_ID=dataset_id)
concept = pd.read_gbq(concept,dialect="standard")
# -

concept.head()

# # f) Which site(s) doe these records originiate from?

# +
#list src_table_id's and counts to see where these records originated from:

src_table = '''

SELECT DISTINCT m.src_table_id, COUNT(*) as count
FROM {DATASET_ID}.procedure_occurrence p
JOIN {DATASET_ID}._mapping_procedure_occurrence m
ON p.procedure_occurrence_id = m.procedure_occurrence_id 
WHERE p.procedure_source_concept_id in (42628107, 42628103, 42628379, 4320851, 4320730, 4320556, 4320557, 4320729, 4320558, 42628410, 42628290, 42628380, 42628118, 42628329, 42628330, 42628111, 42628285, 42628101, 2007440, 45888282, 45888253, 42628263, 42628175, 42628176, 42628318, 42739578, 42628371, 42628204, 42628313, 42628403, 42739595, 42739597, 42628177, 42628178, 42628319, 42628412, 42628413, 42628280, 42628429, 42628168, 42628398, 42628119, 42628435, 42628184, 42739579, 42628320, 42628110, 42628384, 42628126, 42628315, 42628406, 42628191, 42628151, 42628116, 42628117, 42628167, 42628414, 926912, 926913, 42628247, 42628440, 42628261, 42739586, 724952, 724951, 724950, 42628297, 42628409, 45888264, 42628123, 926916, 42739676, 42739675, 42739582, 42739588, 42628389, 42628434, 42628185, 42628121, 42628390, 42628255, 42628188, 42628250, 42628291, 42628157, 42628156, 42628328, 42628358, 42628279, 42628370, 42628236, 42628138, 42628099, 42628268, 42628098, 42628405, 42628137, 42628404, 42628183, 926908, 926910, 42628155, 42628262, 42628396, 42628324, 42628273, 42628286, 926914, 926915, 42628271, 42628321, 42628272, 42628419, 42628190, 42628274, 42628362, 42628147, 42628430, 42628257, 42628251, 42628312, 42628400, 42628170, 42628431, 42628115, 42628249, 42628248, 42628288, 42628114, 42628392, 42628253, 42628287, 724947, 42628129, 42628322, 42628323, 42628304, 42739572, 42628309, 42628310, 42628182, 42628113, 42628198, 42628146, 42628124, 42628332, 42628241, 42628411, 42628374, 42628316, 42628102, 42628425, 42628375, 42628104, 42628424, 42628395, 42628382, 42628293, 42628256, 45888252, 45888279, 45888280, 45888262, 45888275, 45888285, 45888266, 45888276, 45888251, 45888283, 45888258, 45888278, 45888265, 45888270, 42628428, 42628192, 42628152, 42628331, 42628289, 42628153, 42628433, 45888255, 45888257, 42628166, 42739577, 42628386, 42628265, 42628133, 42628365, 42628202, 42628366, 42628131, 42628266, 42628364, 42628447, 42628200, 42628132, 42628401, 42628201, 42628302, 42628194, 42628237, 42628363, 42628393, 42628421, 926911, 42628294, 42628443, 42628145, 926917, 42739596, 42628242, 42628180, 42628439, 42628418, 42628105, 42628179, 42628325, 42628417, 42628387, 42739599, 42740179, 42628277, 42739580, 42628122, 42628436, 42628158, 42628295, 42628144, 42628408, 724953, 42628326, 42628246, 42628449, 42628303, 42628196, 42628264, 42628160, 42628356, 42628327, 42628381, 42628376, 926909, 42628193, 42628416, 42628164, 45888284, 2102799, 2106116, 2108523, 2213392, 42628307, 724949, 42628306, 42628373, 42628240, 42628269, 42628239, 42628238, 42628142, 42628333, 42628335, 42628334, 42739584, 42628276, 42628426, 42739585, 42628162, 42628163, 42628300, 45888263, 42628314, 42628139, 42628141, 42628140, 42628445, 42740440, 42739576, 42628130, 42628391, 42628437, 45888261, 42628108, 42628260, 42628301, 42739581, 42739598, 42628100, 724987, 42628305, 42628127, 42628252, 42739976, 42739592, 42739591, 42628195, 42628441, 42628186, 42628292, 42628154, 42628254, 45888254, 45888274, 45888273, 45888286, 45888267, 45888272, 45888260, 45888259, 45888268, 45888269, 45888277, 45888271, 42628308, 42628296, 42628397, 42628399, 42628360, 42628165, 42628120, 42628361, 42628367, 42628173, 42628368, 42628298, 42628125, 42628438, 42628181, 42628378, 42628275, 42628415, 42628243, 42628244, 42628278, 42628448, 42628128, 42628442, 42628267, 42628134, 42628203, 42628135, 42628357, 42739575, 42628161, 42628187, 42628258, 42628171, 42628388, 42628432, 42628174, 42628199, 42628311, 42628427, 42628372, 42739587, 42628359, 42628444, 42628143, 42628112, 42739583, 42628259, 42628159, 42739590, 42628097, 42628446, 42628270, 42628317, 42628377, 42628394, 42628197, 42628385, 42628420, 42628423, 42628169, 42628299, 42628136, 42739589, 42628106, 42739593, 42739574, 42739594, 42739573, 42628369, 45888281, 45888256, 42628402, 42628172, 42628422, 42628109, 42628383, 42628245, 42628189, 926907, 724954)
GROUP BY m.src_table_id 

'''.format(DATASET_ID=dataset_id)
src_table = pd.read_gbq(src_table,dialect="standard")
# -

src_table

# # g) What procedure_concept_ids are associated? (should these records be deleted or updated)

# +
#list procedure_concept_id's and counts to help determine if these records procedure_concept_id's are valid and if
#these records should be updated rather than deleted

procedure_concept = '''

SELECT DISTINCT p.procedure_concept_id, c.concept_name, COUNT(*) AS count
FROM {DATASET_ID}.procedure_occurrence p
JOIN {DATASET_ID}.concept c ON p.procedure_source_concept_id = c.concept_id
WHERE p.procedure_source_concept_id IN (42628107, 42628103, 42628379, 4320851, 4320730, 4320556, 4320557, 4320729, 4320558, 42628410, 42628290, 42628380, 42628118, 42628329, 42628330, 42628111, 42628285, 42628101, 2007440, 45888282, 45888253, 42628263, 42628175, 42628176, 42628318, 42739578, 42628371, 42628204, 42628313, 42628403, 42739595, 42739597, 42628177, 42628178, 42628319, 42628412, 42628413, 42628280, 42628429, 42628168, 42628398, 42628119, 42628435, 42628184, 42739579, 42628320, 42628110, 42628384, 42628126, 42628315, 42628406, 42628191, 42628151, 42628116, 42628117, 42628167, 42628414, 926912, 926913, 42628247, 42628440, 42628261, 42739586, 724952, 724951, 724950, 42628297, 42628409, 45888264, 42628123, 926916, 42739676, 42739675, 42739582, 42739588, 42628389, 42628434, 42628185, 42628121, 42628390, 42628255, 42628188, 42628250, 42628291, 42628157, 42628156, 42628328, 42628358, 42628279, 42628370, 42628236, 42628138, 42628099, 42628268, 42628098, 42628405, 42628137, 42628404, 42628183, 926908, 926910, 42628155, 42628262, 42628396, 42628324, 42628273, 42628286, 926914, 926915, 42628271, 42628321, 42628272, 42628419, 42628190, 42628274, 42628362, 42628147, 42628430, 42628257, 42628251, 42628312, 42628400, 42628170, 42628431, 42628115, 42628249, 42628248, 42628288, 42628114, 42628392, 42628253, 42628287, 724947, 42628129, 42628322, 42628323, 42628304, 42739572, 42628309, 42628310, 42628182, 42628113, 42628198, 42628146, 42628124, 42628332, 42628241, 42628411, 42628374, 42628316, 42628102, 42628425, 42628375, 42628104, 42628424, 42628395, 42628382, 42628293, 42628256, 45888252, 45888279, 45888280, 45888262, 45888275, 45888285, 45888266, 45888276, 45888251, 45888283, 45888258, 45888278, 45888265, 45888270, 42628428, 42628192, 42628152, 42628331, 42628289, 42628153, 42628433, 45888255, 45888257, 42628166, 42739577, 42628386, 42628265, 42628133, 42628365, 42628202, 42628366, 42628131, 42628266, 42628364, 42628447, 42628200, 42628132, 42628401, 42628201, 42628302, 42628194, 42628237, 42628363, 42628393, 42628421, 926911, 42628294, 42628443, 42628145, 926917, 42739596, 42628242, 42628180, 42628439, 42628418, 42628105, 42628179, 42628325, 42628417, 42628387, 42739599, 42740179, 42628277, 42739580, 42628122, 42628436, 42628158, 42628295, 42628144, 42628408, 724953, 42628326, 42628246, 42628449, 42628303, 42628196, 42628264, 42628160, 42628356, 42628327, 42628381, 42628376, 926909, 42628193, 42628416, 42628164, 45888284, 2102799, 2106116, 2108523, 2213392, 42628307, 724949, 42628306, 42628373, 42628240, 42628269, 42628239, 42628238, 42628142, 42628333, 42628335, 42628334, 42739584, 42628276, 42628426, 42739585, 42628162, 42628163, 42628300, 45888263, 42628314, 42628139, 42628141, 42628140, 42628445, 42740440, 42739576, 42628130, 42628391, 42628437, 45888261, 42628108, 42628260, 42628301, 42739581, 42739598, 42628100, 724987, 42628305, 42628127, 42628252, 42739976, 42739592, 42739591, 42628195, 42628441, 42628186, 42628292, 42628154, 42628254, 45888254, 45888274, 45888273, 45888286, 45888267, 45888272, 45888260, 45888259, 45888268, 45888269, 45888277, 45888271, 42628308, 42628296, 42628397, 42628399, 42628360, 42628165, 42628120, 42628361, 42628367, 42628173, 42628368, 42628298, 42628125, 42628438, 42628181, 42628378, 42628275, 42628415, 42628243, 42628244, 42628278, 42628448, 42628128, 42628442, 42628267, 42628134, 42628203, 42628135, 42628357, 42739575, 42628161, 42628187, 42628258, 42628171, 42628388, 42628432, 42628174, 42628199, 42628311, 42628427, 42628372, 42739587, 42628359, 42628444, 42628143, 42628112, 42739583, 42628259, 42628159, 42739590, 42628097, 42628446, 42628270, 42628317, 42628377, 42628394, 42628197, 42628385, 42628420, 42628423, 42628169, 42628299, 42628136, 42739589, 42628106, 42739593, 42739574, 42739594, 42739573, 42628369, 45888281, 45888256, 42628402, 42628172, 42628422, 42628109, 42628383, 42628245, 42628189, 926907, 724954)
GROUP BY p.procedure_concept_id, c.concept_name

'''.format(DATASET_ID=dataset_id)
procedure_concept = pd.read_gbq(procedure_concept,dialect="standard")
# -

procedure_concept

# # h) How would you fix the issue?
# My strategy to resolve this issue is that I will update values in the 'modifier_concept_id' field of the impacted rows with values in the 'procedure_source_concept_id' field.


