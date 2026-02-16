# Project-agnositic Re-usable helper Functions 

Repo holds functions/modules that can be repurposed or reusable across various projects.

**duplicate_finder_text**

Problem: Multiple similar data points upon which analysis has got to be performed, and compressing the data into a set of unique data points doesn't in anyway change analysis outcome.

Solution: Compute clusters based on semantic similarity relying on cosine similarity to compute the semantic distance or similarity between data points. Resultant data structure has each data point grouped into a cluster , and therefore you can chose to retain a single datapoint from each cluster upon which to do your analysis. 

Benefits:
> - Efficient large data analysis 
> - Significant reduction in compute time

Example of 
<div style="display: flex; align-items: center; justify-content: center; gap: 10px;">

<div>

**Table 1: Original Data**

| index |          Notes           |
|:-----:|:------------------------:|
|   0   | University of Liverpool  |
|   1   |  Manchester University   |
|   2   |   Liverpool University   |
|   3   |  The Univ. of Liverpool  |
|   4   |           UoM            |
|   5   | The Univ. of Manchester  |
|   6   | University of Manchester |
|   7   |        Microsoft         |



</div>

<div style="font-size: 1em;">→</div>

<div>

**Table 2: Clustered Data**

| index |          Notes           | Semantic Cluster |
|:-----:|:------------------------:|:----------------:|
|   0   | University of Liverpool  |        0         |
|   1   |  Manchester University   |        1         |
|   2   |   Liverpool University   |        0         |
|   3   |  The Univ. of Liverpool  |        0         |
|   4   | The Univ. of Manchester  |        1         |  
|   5   |           UoM            |        1         |
|   6   | University of Manchester |        1         |
|   7   |        Microsoft         |        2         |

</div>

</div>