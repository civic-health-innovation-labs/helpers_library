# Project-agnositic Re-usable helper Functions 

Repo holds functions/modules that can be repurposed or reusable across various projects.

**duplicate_finder_text**

Problem: Multiple similar data points upon which analysis has got to be performed, and compressing the data into a set of unique data points doesn't in anyway change analysis outcome.

Solution: Compute clusters based on semantic similarity relying on cosine similarity to compute the semantic distance or similarity between data points. Resultant data structure has each data point grouped into a cluster , and therefore you can chose to retain a single datapoint from each cluster upon which to do your analysis. 

Benefits:
> - Efficient large data analysis 
> - Significant reduction in compute time