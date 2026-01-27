from __future__ import annotations
from typing import Dict
import pathlib

try:
    import zoo
except ImportError:
    # Use centralized ZooStub from zoo-runner-common package
    from zoo_runner_common.zoostub import ZooStub
    zoo = ZooStub()

import json
import os
import sys

import requests
import yaml
from loguru import logger
from pystac import Catalog, Collection, read_file
from pystac.item_collection import ItemCollection
from pystac.stac_io import StacIO
from zoo_wes_runner import ZooWESRunner
from zoo_template_common import CommonExecutionHandler, CustomStacIO

# For DEBUG
import traceback

logger.remove()
logger.add(sys.stderr, level="INFO")

StacIO.set_default(CustomStacIO)


class WESRunnerExecutionHandler(CommonExecutionHandler):
    """
    Execution handler for ZOO WES Runner with STAC catalog and S3 support.

    Inherits from CommonExecutionHandler to provide a standardized interface
    compatible with the zoo-template-common package. Handles post-execution
    processing of STAC catalogs and output management.
    """

    def __init__(self, conf=None, outputs=None, **kwargs):
        super().__init__(conf=conf, **kwargs)
        self.outputs = outputs or {}
        self.http_proxy_env = os.environ.get("HTTP_PROXY", None)
        self.job_id = None
        # Initialize namespace name for cases where parent __init__ may not set it
        # (workaround for ZooWESRunner inheritance chain issue)
        if not hasattr(self, '_namespace_name'):
            self._namespace_name = None

    def unset_http_proxy_env(self):
        """Temporarily unset HTTP_PROXY environment variable."""
        http_proxy = os.environ.pop("HTTP_PROXY", None)
        logger.info(f"Unsetting env HTTP_PROXY, whose value was {http_proxy}")

    def restore_http_proxy_env(self):
        """Restore HTTP_PROXY environment variable if it was set."""
        if self.http_proxy_env:
            os.environ["HTTP_PROXY"] = self.http_proxy_env
            logger.info(f"Restoring env HTTP_PROXY, to value {self.http_proxy_env}")

    def post_execution_hook(self, log, output, usage_report, tool_logs):
        try:
            logger.info("Post execution hook")
            self.unset_http_proxy_env()

            logger.info("Set user bucket settings")
            additional_params = self.get_additional_parameters()
            os.environ["AWS_S3_ENDPOINT"] = additional_params.get("endpoint_url", "")
            os.environ["AWS_ACCESS_KEY_ID"] = additional_params.get("aws_access_key_id", "")
            os.environ["AWS_SECRET_ACCESS_KEY"] = additional_params.get("aws_secret_access_key", "")
            os.environ["AWS_REGION"] = additional_params.get("region_name", "")

            StacIO.set_default(CustomStacIO)

            logger.info(f"Output received: {json.dumps(output, indent=2, default=str)}")

            for i in self.outputs:
                logger.info(f"Processing output {i}")
                if i in output:
                    self.setOutput(i, output)
                else:
                    logger.warning(f"Output {i} not found in workflow output")
                    self.outputs[i]["value"] = json.dumps({"type": "FeatureCollection", "features": []})

        except Exception as e:
            logger.error("ERROR in post_execution_hook...")
            logger.error(traceback.format_exc())
            raise e

        finally:
            self.restore_http_proxy_env()

    def setOutput(self, outputName, values):
        logger.info(f"Processing output '{outputName}' from workflow results")
        output = self.outputs[outputName]

        # Extract the output value - could be string, dict with 'path'/'value', or dict with 'class': 'Directory'
        output_value = values[outputName]
        
        if isinstance(output_value, dict):
            # CWL Directory object or similar
            stac_path = output_value.get("path", output_value.get("value", str(output_value)))
        else:
            stac_path = str(output_value)

        logger.info(f"Read catalog from STAC Catalog URI: {stac_path}")

        # Handle list of outputs
        if not isinstance(values.get(outputName), list):
            logger.info(f"values[{outputName}] is not a list, transform to an array")
            values[outputName] = [values[outputName]]

        items = []
        collection = None

        for catalog_ref in values[outputName]:
            if catalog_ref is None:
                break
            
            # Extract path from catalog reference
            if isinstance(catalog_ref, dict):
                s3_path = catalog_ref.get("path", catalog_ref.get("value", str(catalog_ref)))
            else:
                s3_path = str(catalog_ref)

            try:
                # Ensure S3 path format
                if not s3_path.startswith("s3://"):
                    s3_path = "s3://" + s3_path
                
                logger.info(f"Reading STAC catalog from: {s3_path}")
                cat: Catalog = read_file(s3_path)
                logger.info("Catalog read successfully")
                
            except Exception as e:
                logger.error(f"Failed to read catalog from {s3_path}: {str(e)}")
                logger.debug(traceback.format_exc())
                # Continue with empty collection rather than failing completely
                output["value"] = json.dumps({"type": "FeatureCollection", "features": []})
                return

            collection_id = self.get_additional_parameters().get("sub_path", "unknown")
            logger.info(f"Create collection with ID {collection_id}")

            # Try to get collection from catalog
            try:
                logger.info(f"Attempting to extract collection from catalog")
                collection: Collection = next(cat.get_all_collections())
                logger.info(f"Found collection: {collection.id}")
            except StopIteration:
                logger.info("No pre-existing collection found, processing items directly")
                try:
                    items_from_cat = list(cat.get_all_items())
                    logger.info(f"Found {len(items_from_cat)} items in catalog")
                    
                    itemFinal = []
                    for item in items_from_cat:
                        try:
                            for a in item.assets.keys():
                                cDict = item.assets[a].to_dict()
                                cDict["storage:platform"] = "EOEPCA"
                                cDict["storage:requester_pays"] = False
                                cDict["storage:tier"] = "Standard"
                                cDict["storage:region"] = self.get_additional_parameters().get("region_name", "")
                                cDict["storage:endpoint"] = self.get_additional_parameters().get("endpoint_url", "")
                                item.assets[a] = item.assets[a].from_dict(cDict)
                            
                            item.collection_id = collection_id
                            itemFinal.append(item.clone())
                            items.append(item.clone())
                        except Exception as item_e:
                            logger.error(f"Error processing item: {str(item_e)}")
                            logger.debug(traceback.format_exc())
                            continue
                    
                    if itemFinal:
                        collection = ItemCollection(items=itemFinal)
                        logger.info(f"Created ItemCollection from {len(itemFinal)} items")
                    
                except Exception as e:
                    logger.error(f"Error processing items from catalog: {str(e)}")
                    logger.debug(traceback.format_exc())

        # Trap the case of no output collection
        if collection is None:
            logger.warning("No collection found, creating empty ItemCollection")
            collection = ItemCollection(items=[])

        if len(items) > 0 and isinstance(collection, list):
            collection = ItemCollection(items=items)
        
        collection_dict = collection.to_dict()
        collection_id = self.get_additional_parameters().get("sub_path", "unknown")
        collection_dict["id"] = collection_id
        
        output["value"] = json.dumps(collection_dict, indent=2)
        logger.info("Output successfully set")

    def local_get_file(self, fileName):
        try:
            with open(fileName) as yaml_file:
                yaml_data = yaml.safe_load(yaml_file)
                logger.info(f"Loaded YAML file: {fileName}")
                return yaml_data
        except FileNotFoundError:
            logger.error(f"File not found: {fileName}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {fileName}: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error loading file {fileName}: {str(e)}")
            return {}

    def set_job_id(self, job_id):
        """Set the job identifier."""
        self.job_id = job_id
        logger.info(f"Job ID set to: {job_id}")

    def get_namespace(self) -> str:
        return None

    def get_pod_env_vars(self):
        """Get pod environment variables configuration."""
        logger.info("Getting pod environment variables")
        return {}

    def get_pod_node_selector(self):
        """Get pod node selector configuration."""
        logger.info("Getting pod node selector")
        return {}

    def get_secrets(self):
        """Load image pull secrets from configuration."""
        logger.info("Getting secrets")
        return []

    def get_additional_parameters(self):
        additional_params = self.conf.get("additional_parameters", {}).copy()

        # Preserve config values, only fall back to environment if missing
        additional_params["region_name"] = additional_params.get(
            "region_name",
            os.environ.get("AWS_REGION", "us-east-1"),
        )
        additional_params["endpoint_url"] = additional_params.get(
            "endpoint_url",
            os.environ.get("AWS_S3_ENDPOINT", "http://localhost:9000"),
        )
        additional_params["aws_access_key_id"] = additional_params.get(
            "aws_access_key_id",
            os.environ.get("AWS_ACCESS_KEY_ID", ""),
        )
        additional_params["aws_secret_access_key"] = additional_params.get(
            "aws_secret_access_key",
            os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        )

        # Set sub_path from USID if available
        usid = self.conf.get("lenv", {}).get("usid", "default")
        additional_params["sub_path"] = usid

        logger.info(f"Additional parameters configured: {list(additional_params.keys())}")
        return additional_params

    def handle_outputs(self, log, output, usage_report, tool_logs):
        tmpPath = self.conf.get("main", {}).get("tmpPath", "/tmp")
        jobDir = os.path.join(tmpPath, self.job_id) if self.job_id else tmpPath
        
        os.makedirs(jobDir, mode=0o777, exist_ok=True)
        
        try:
            # Write execution logs
            with open(os.path.join(jobDir, "job.log"), "w") as f:
                f.write(log if isinstance(log, str) else "")
            
            # Write raw results
            with open(os.path.join(jobDir, "output.json"), "w") as output_file:
                json.dump(output or {}, output_file, indent=4)
            
            # Write usage report
            with open(os.path.join(jobDir, "usage-report.json"), "w") as usage_report_file:
                json.dump(usage_report or {}, usage_report_file, indent=4)
            
            # Create aggregated report
            aggregated_outputs = {
                "usage_report": usage_report or {},
                "outputs": output or {},
                "log": os.path.join(self.job_id, "job.log") if self.job_id else "job.log",
            }
            
            with open(os.path.join(jobDir, "report.json"), "w") as report_file:
                json.dump(aggregated_outputs, report_file, indent=4)
            
            logger.info(f"Output files written to {jobDir}")
            
        except Exception as e:
            logger.error(f"Error handling outputs: {str(e)}")
            logger.error(traceback.format_exc())


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs):  # noqa
    runner = None
    execution_handler = None
    
    try:
        logger.info("Starting CWL workflow execution")
        
        # Load CWL package
        cwl_path = os.path.join(
            pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
            "app-package.cwl",
        )
        
        if not os.path.exists(cwl_path):
            raise FileNotFoundError(f"CWL package not found at {cwl_path}")
        
        logger.info(f"Loading CWL from {cwl_path}")
        with open(cwl_path, "r") as stream:
            cwl = yaml.safe_load(stream)
        logger.info("CWL loaded successfully")

        # Create execution handler
        execution_handler = WESRunnerExecutionHandler(conf=conf, outputs=outputs)
        logger.info("Execution handler created")

        # Create and configure WES runner
        runner = ZooWESRunner(
            cwl=cwl,
            conf=conf,
            inputs=inputs,
            outputs=outputs,
            execution_handler=execution_handler,
        )

        # Set monitoring interval
        runner.monitor_interval = 10
        logger.info("WES runner configured with 10s monitoring interval")

        # Change working directory to store outputs
        working_dir = os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
        os.makedirs(working_dir, mode=0o777, exist_ok=True)
        os.chdir(working_dir)

        logger.info(f"Executing workflow in {working_dir}")

        # Execute workflow
        exit_status = runner.execute()
        logger.info(f"Workflow execution completed with status: {exit_status}")

        # Handle generated logs
        if runner is not None and hasattr(runner, 'run_log_content') and runner.run_log_content is not None:
            log_path = os.path.join(
                conf["main"]["tmpPath"],
                f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}_job.log"
            )
            try:
                with open(log_path, "w+") as f:
                    f.write(runner.run_log_content)

                # Store log metadata in output
                conf["service_logs"] = {
                    "url": os.path.join(
                        conf["main"]["tmpUrl"],
                        f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}_job.log"
                    ),
                    "title": "WES workflow execution log",
                    "rel": "related",
                }
                logger.info(f"Job log saved to {log_path}")
            except Exception as log_e:
                logger.error(f"Failed to save job log: {str(log_e)}")

        # Handle result
        if exit_status == zoo.SERVICE_SUCCEEDED:
            logger.info("Workflow execution succeeded")

            # Return processed STAC results
            for key in outputs:
                if "value" in outputs[key]:
                    logger.info(f"Output {key} has value set")
            
            return zoo.SERVICE_SUCCEEDED

        else:
            conf["lenv"]["message"] = zoo._("Workflow execution failed")
            logger.error("Workflow execution failed")
            return zoo.SERVICE_FAILED

    except FileNotFoundError as fe:
        logger.error(f"File not found: {str(fe)}")
        conf["lenv"]["message"] = zoo._(f"Configuration file error: {str(fe)}")
        return zoo.SERVICE_FAILED
    
    except Exception as e:
        logger.error("ERROR during workflow execution")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception message: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Try to capture and save execution logs
        try:
            if runner is not None:
                log_path = os.path.join(
                    conf["main"]["tmpPath"],
                    f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}_error.log"
                )
                with open(log_path, "w+", encoding="utf-8") as file:
                    file.write("=== EXECUTION ERROR LOG ===\n")
                    file.write(f"Timestamp: {str(traceback.format_exc())}\n")
                    
                    if hasattr(runner, 'run_log_content') and runner.run_log_content:
                        file.write("\n=== RUN LOG ===\n")
                        file.write(runner.run_log_content)
                    
                    if hasattr(runner, 'execution') and runner.execution:
                        if hasattr(runner.execution, 'get_log'):
                            try:
                                file.write("\n=== EXECUTION LOG ===\n")
                                file.write(runner.execution.get_log())
                            except Exception:
                                pass
                
                logger.info(f"Error log saved to {log_path}")
        except Exception as log_e:
            logger.error(f"Failed to save error logs: {str(log_e)}")
        
        error_msg = f"Workflow execution error: {str(e)}"
        conf["lenv"]["message"] = zoo._(error_msg)
        logger.error(f"Service failure message: {error_msg}")
        return zoo.SERVICE_FAILED
