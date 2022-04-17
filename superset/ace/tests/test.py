import requests
import json
import pprint


class TestSuperset:
    def __init__(self):
        self.url_header = "http://localhost:8088/api/v1"
        self.access_token = None
        self.refresh_token = None
        self.headers = None
        self.dash_info_dict = {}
        self.dash_id_to_title = {}
        self.one_dash_dict = {}
        self.charts_dict = {}

        self.data_source_to_chart = {}
        self.chart_id_to_form_data = {}

        self.chart_result_dict = {}
        self.pp = pprint.PrettyPrinter(indent=4)

    def print(self, to_print: str) -> None:
        self.pp.pprint(to_print)

    # The following functions test the endpoints of Superset
    def login(self) -> None:
        login_url = "{url_header}/security/login".format(url_header=self.url_header)
        login_request_body = {
            "username": "admin",
            "password": "admin",
            "provider": "db",
            "refresh": True,
        }
        login_result = requests.post(login_url, json=login_request_body)
        self.print(login_result.text)
        token_dict = json.loads(login_result.text)
        self.access_token = token_dict["access_token"]
        self.refresh_token = token_dict["refresh_token"]
        self.headers = {"Content-type": "application/json",
                        "Authorization": "Bearer {token}".format(
                            token=self.access_token)}

    def get_dash_info(self) -> None:
        get_dash_info_url = "{url_header}/dashboard".format(url_header=self.url_header)
        dash_info_result = requests.get(get_dash_info_url, headers=self.headers)
        self.dash_info_dict = json.loads(dash_info_result.text)
        for dash_info in self.dash_info_dict["result"]:
            dash_id = dash_info["id"]
            dash_title = dash_info["dashboard_title"]
            self.dash_id_to_title[dash_id] = dash_title

    def get_one_dash(self, dash_id: int) -> None:
        get_one_dash_url = "{url_header}/dashboard/{id_or_slug}" \
            .format(url_header=self.url_header, id_or_slug=str(dash_id))
        one_dash_result = requests.get(get_one_dash_url, headers=self.headers)
        self.one_dash_dict = json.loads(one_dash_result.text)
        self.print(self.one_dash_dict)

    def get_charts(self, dash_id: int) -> None:
        get_charts_url = "{url_header}/dashboard/{id_or_slug}/charts" \
            .format(url_header=self.url_header, id_or_slug=str(dash_id))
        charts_result = requests.get(get_charts_url, headers=self.headers)
        self.charts_dict = json.loads(charts_result.text)
        self.print(self.charts_dict)
        for chart_data in self.charts_dict["result"]:
            form_data = chart_data["form_data"]
            data_source_id = int(form_data["datasource"].split("__")[0])
            slice_id = int(form_data["slice_id"])
            slice_id_list = self.data_source_to_chart.get(data_source_id, list())
            slice_id_list.append(slice_id)
            self.chart_id_to_form_data[slice_id] = form_data

    def refresh_one_chart(self, chart_id: int) -> None:
        refresh_chart_url = "{url_header}/chart/data" \
            .format(url_header=self.url_header)
        form_data = self.chart_id_to_form_data[chart_id]
        data_source_str = form_data["datasource"]
        data_source_id = int(data_source_str.split("__")[0])
        data_source_type = data_source_str.split("__")[1]
        metrics = []
        if form_data.get("metric", None) is not None:
            metrics.append(form_data.get("metric"))
        if form_data.get("metrics", None) is not None:
            metrics.extend(form_data.get("metrics"))
        order_by = [[metric, False] for metric in metrics]
        request_body = {
            "datasource": {"id": data_source_id, "type": data_source_type},
            "force": False,
            "result_format": "json",
            "result_type": "full",
            "queries": [{
                "time_range": form_data["time_range"],
                "filters": [],
                "extra": {
                    "time_range_endpoints": ["inclusive", "exclusive"],
                    "having": "",
                    "having_druid": [],
                    "where": "",
                },
                "applied_time_extra": {},
                "columns": form_data['groupby'],
                "metrics": metrics,
                "orderby": order_by,
                "annotation_layers": [],
                "row_limit": 10000,
                "timeseries_limit": 0,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "group_by": form_data['groupby'],
            }]
        }
        chart_result = requests.post(refresh_chart_url,
                                     headers=self.headers,
                                     json=request_body)

        self.chart_result_dict = json.loads(chart_result.text)
        self.print(self.chart_result_dict)

    # The following functions test
    def load_one_dash_ace(self, dash_id: int):
        get_one_dash_url = "{url_header}/dashboard/ace/{id_or_slug}" \
            .format(url_header=self.url_header, id_or_slug=str(dash_id))
        one_dash_result = requests.get(get_one_dash_url, headers=self.headers)
        self.one_dash_dict = json.loads(one_dash_result.text)
        self.print(self.one_dash_dict)
