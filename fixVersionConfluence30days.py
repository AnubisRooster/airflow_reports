from atlassian import Confluence
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.atlassian.jira.hooks.jira import JiraHook

from datetime import datetime

def fix_version_30day():
    # Jira Configuration
    jira_url = Variable.get("JIRA_URL")
    jira_username = Variable.get("JIRA_USERNAME")
    jira_api_token = Variable.get("JIRA_API_TOKEN")
    jira_project_key = Variable.get("JIRA_PROJECT_KEY")
    custom_field_id = Variable.get("CUSTOM_FIELD_ID")

    # Confluence Configuration
    confluence_url = Variable.get("CONFLUENCE_URL")
    confluence_username = Variable.get("CONFLUENCE_USERNAME")
    confluence_api_token = Variable.get("JIRA_API_TOKEN")
    confluence_page_id = Variable.get("CONFLUENCE_PAGE_ID_30DAY") #updated variable

    # Initialize Jira
    # jira = jc.JIRA(server=jira_url, basic_auth=(jira_username, jira_api_token))

    jira = JiraHook(jira_conn_id='jira-user').get_conn()

    # Query Jira for issues updated in the last two weeks
    query = f'project = {jira_project_key} AND issuetype = Features AND fixVersion CHANGED AFTER startOfDay(-30d) AND key != PMZ-721' #updated query
    issues = jira.jql(query, fields="summary,status,fixVersions,created,components,customfield_13085")

    # Calculate date seven days ago
    seven_days_ago = (datetime.now() - timedelta(days=30))

    # Format the date as "Month Day, Year"
    formatted_date = seven_days_ago.strftime('%B %d, %Y')   

    # Initialize a list to store extracted results
    results = []

    # Iterate through the fetched issues
    for issue in issues.get('issues'):

        issue_key = issue.get('key')
        summary = issue.get('fields').get('summary')
        created_date = issue.get('fields').get('created')[:10]
        components = issue.get('fields').get('components')

        # Fetch issue changelogs
        changelogs = jira.issue(issue_key, expand='changelog').get('changelog').get('histories')

        # Initialize variables to store previous values
        prev_fix_versions = []

        # Extract change details
        for change in changelogs:
            for item in change.get('items'):
                if item.get('field') == 'Fix Version':
                    # Check if the 'fromString' attribute exists in the item and it's not None
                    # if hasattr(item, 'fromString') and item.fromString is not None:
                    if item.get('fromString') is not None:
                        prev_fix_versions.append(item.get('fromString'))

        # Get the most recent Fix Version value from the changelog
        most_recent_prev_fix_version = prev_fix_versions[-1] if prev_fix_versions else None

        # Get the current "fixVersions" associated with the Jira issue
        current_fix_versions = [fix_version.get('name') for fix_version in
                                issue.get('fields').get('fixVersions')] if issue.get('fields').get(
            'fixVersions') else []

        # Convert components objects to a list of component names
        component_names = ', '.join([component.get('name') for component in components]) if components else ''

        # Access the custom field 'customfield_13085' (User Picker - Single User)
        engman_value = issue.get('fields').get(custom_field_id)


        # Check if there are changes to the fix version
        if most_recent_prev_fix_version is not None:
            # Add the relevant information to the results list
            results.append({
                'key': issue_key,
                'summary': summary,
                'status': issue.get('fields').get('status').get('name'),
                'created_date': created_date,
                'previous_fix_versions': most_recent_prev_fix_version,
                'current_fix_versions': ', '.join(current_fix_versions),
                'components': component_names,
                'engman': engman_value.get('displayName') if engman_value else '',  # Get the display name of the user
            })

    # Initialize Confluence
    confluence = Confluence(
        url=confluence_url,
        username=confluence_username,
        password=confluence_api_token)

    # Construct the content for the Confluence page
    page_content = f"Updated Issue Fix Versions Since {seven_days_ago}\n\n"

    # Add a table with headers
    page_content += "|| Issue Key || Summary || Status || Created Date || Previous Fix Versions || Current Fix Versions || Component || Engineering Manager ||\n"

    # Add rows to the table with issue details
    for result in results:
        row = (
            f"| {result['key']} | {result['summary']} | {result['status']} | "
            f"{result['created_date']} | {result['previous_fix_versions']} | {result['current_fix_versions']} | {result['components']} | {result['engman']} |\n"
        )
        page_content += row

    # Update the Confluence page
    confluence.update_page(
        page_id=confluence_page_id,
        title=f"PMZ Fix Versions Changes - 30 Day Lookback",
        body=page_content,
        representation='wiki'
    )

@dag(dag_id='Jira-FixVersionConfluence', schedule_interval=None, start_date=datetime(2023, 9, 1), catchup=False) # @Tejaswa - update dag id?
def base_function():
    execute_fix_version_30day = PythonOperator(
        task_id='ExecuteCode',
        python_callable=fix_version_30day
    )

    execute_fix_version_30day

base_function()
