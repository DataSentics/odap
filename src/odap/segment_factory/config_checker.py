import os
import yaml

USE_CASES_FOLDER = "use_cases"


def check_destinations_exist():
    main_config = load_main_config()
    use_cases = [
        use_case for use_case in os.listdir(USE_CASES_FOLDER) if os.path.isdir(os.path.join(USE_CASES_FOLDER, use_case))
    ]

    for use_case in use_cases:
        use_case_config = load_use_case_config(use_case)
        use_case_destinations = [export_spec["destination"] for _, export_spec in use_case_config["exports"].items()]
        missing_destinations = set(use_case_destinations) - set(main_config["destinations"])

        if len(missing_destinations) > 0:
            raise RuntimeError(
                f"Destinations {missing_destinations} for use case '{use_case}' are not present in config.yaml"
            )


def load_main_config():
    with open("config.yaml", mode="r", encoding="utf-8") as f:
        return yaml.load(f, Loader=yaml.FullLoader)["parameters"]["segmentfactory"]


def load_use_case_config(use_case: str):
    use_case_config_path = os.path.join(USE_CASES_FOLDER, use_case, "config.yaml")

    with open(use_case_config_path, mode="r", encoding="utf-8") as f:
        return yaml.load(f, Loader=yaml.FullLoader)["parameters"]
