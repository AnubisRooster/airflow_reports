This is series of python scripts that scrapes Jira for project activity over various periods of time, determines which items have shifted from one fix version to another, then places the results on a designated Confluence page for the Product team to review.

Airflow helps to schedule the report frequency as well as capture the variables needed to execute the script. 

There are four different versions of this script available:

1) 1 Week look back.
2) 2 Week look back.
3) 30 Day look back.
4) 90 Day look back.

If you are using Airflow, you will need to set up the Variables under the Admin console. API keys will be masked, naturally, but the other variables need to be declared.

