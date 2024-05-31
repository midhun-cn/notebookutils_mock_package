import concurrent.futures        
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbparameterise import extract_parameters, parameter_values, replace_definitions


class mssparkutils:
    
    class runtime:
        context = {
            'currentNotebookName': 'NFW_POC_CN_DAG_Orchestrator',
            'sparkpool': '1bb404f2-51d1-52f6-5620-645b87610270',
            'parentRunId': None,
            'hcReplId': None,
            'currentWorkspaceId': 'd6c785fb-2682-4b16-b670-f4f19ef28861',
            'referenceTreePath': None,
            'workspace': '55dynamicdatacloudworkspace',
            'jobId': '481',
            'currentNotebookId': '4e35e36f-1ce9-40bb-8f0c-2ef56d551b55',
            'currentRunId': None,
            'isForPipeline': False,
            'rootRunId': None
        }

    class notebook:

        def execute_notebook(notebook_path, **kwargs):
            # Load the notebook
            with open(notebook_path+'.ipynb') as f:
                nb = nbformat.read(f, as_version=4)

            # Extract parameters
            orig_parameters = extract_parameters(nb)

            # Replace parameters
            new_parameters = parameter_values(orig_parameters, **kwargs)
            new_nb = replace_definitions(nb, new_parameters)

            # Execute the notebook
            ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
            ep.preprocess(new_nb)

            # Check the output of all the cells
            for cell in new_nb.cells:
                if cell.cell_type == 'code' and cell.outputs:
                    for output in cell.outputs:
                        if output.output_type == 'error':
                            raise Exception(output.evalue)

        @staticmethod
        def run_task(task):
            # Simulate task execution
            mssparkutils.notebook.execute_notebook(task['path'], **task['args'])
            # Add your actual task execution code here
        
        @staticmethod
        def runMultiple(dag):
            # Parse the JSON
            activities = dag["activities"]

            # Build a dependency graph
            tasks = activities
            graph = {task['name']: set(task.get('dependencies', [])) for task in tasks}
            dependents = {task['name']: {t['name'] for t in tasks if task['name'] in t.get('dependencies', [])} for task in tasks}
            results = {}

            def mark_as_failed(task_name, exception):
                if task_name in results and results[task_name]['status'] == 'FAILURE':
                    return
                results[task_name] = {"status": "FAILURE", "exception": exception}
                for dependent in dependents[task_name]:
                    mark_as_failed(dependent, "Dependent task failed")

            while graph:
                # Find tasks with no dependencies
                ready_tasks = [name for name, deps in graph.items() if not deps]

                if not ready_tasks:  # Circular dependency
                    raise Exception("Circular dependency detected")

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = {executor.submit(mssparkutils.notebook.run_task, task): task for task in tasks if task['name'] in ready_tasks}

                    for future in concurrent.futures.as_completed(futures):
                        task = futures[future]
                        try:
                            future.result()
                        except Exception as exc:
                            mark_as_failed(task['name'], str(exc))
                        else:
                            if task['name'] not in results:
                                results[task['name']] = {"status": "SUCCESS", "exception": "None"}

                # Remove completed tasks from the graph
                for task_name in ready_tasks:
                    graph.pop(task_name)
                    for deps in graph.values():
                        deps.discard(task_name)

            return results


