import base64
import importlib
import json
import os
import pathlib

import yaml
import zoo
import zoo_wes_runner
from zoo_calrissian_runner.handlers import ExecutionHandler
from zoo_wes_runner import ZooWESRunner


class WESRunnerExecutionHandler(ExecutionHandler):
    def get_additional_parameters(self):
        return {
            "ADES_STAGEOUT_AWS_SERVICEURL": os.getenv("AWS_SERVICE_URL", None),
            "ADES_STAGEOUT_AWS_REGION": os.getenv("AWS_REGION", None),
            "ADES_STAGEOUT_AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", None),
            "ADES_STAGEOUT_AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY", None
            ),
            "ADES_STAGEIN_AWS_SERVICEURL": os.getenv("AWS_SERVICE_URL", None),
            "ADES_STAGEIN_AWS_REGION": os.getenv("AWS_REGION", None),
            "ADES_STAGEIN_AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", None),
            "ADES_STAGEIN_AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY", None
            ),
            "ADES_STAGEOUT_OUTPUT": os.getenv("ADES_STAGEOUT_OUTPUT", None)
        }

    def handle_outputs(self, log, output, usage_report, tool_logs):
        
        os.makedirs(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id),
            mode=0o777,
            exist_ok=True,
        )
        with open(os.path.join(self.conf["main"]["tmpPath"], self.job_id, "job.log"), "w") as f:
            f.writelines(log)

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "output.json"), "w"
        ) as output_file:
            json.dump(output, output_file, indent=4)

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "usage-report.json"),
            "w",
        ) as usage_report_file:
            json.dump(usage_report, usage_report_file, indent=4)

        aggregated_outputs = {}
        aggregated_outputs = {
            "usage_report": usage_report,
            "outputs": output,
            "log": os.path.join(self.job_id, "job.log"),
        }

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "report.json"), "w"
        ) as report_file:
            json.dump(aggregated_outputs, report_file, indent=4)


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs):

    with open(
        os.path.join(
            pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
            "app-package.cwl",
        ),
        "r",
    ) as stream:
        cwl = yaml.safe_load(stream)

    runner = ZooWESRunner(
        cwl=cwl,
        conf=conf,
        inputs=inputs,
        outputs=outputs,
        execution_handler=WESRunnerExecutionHandler(conf=conf),
    )
    exit_status = runner.execute()

    if exit_status == zoo.SERVICE_SUCCEEDED:
        # TODO remove hardcoded key StacCatalogUri which is defined in the main.cwl
        # Remove the "stac" output from runner.outputs.outputs["stac"]["value"] in previous phases
        out = {"StacCatalogUri": runner.outputs.outputs["stac"]["value"]["StacCatalogUri"] }
        json_out_string= json.dumps(out, indent=4)
        outputs["stac"]["value"]=json_out_string
        return zoo.SERVICE_SUCCEEDED

    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
