from datetime import datetime, timedelta
from itertools import chain
import dateparser
import numpy as np
import pandas as pd
import streamlit as st


class Dataloader:
    def __init__(self, teamleader_client, teamleader_v1_client):
        self.tl1 = teamleader_v1_client
        self.tl = teamleader_client

    @st.cache
    def load_raw_invoice_data(self, nrows):
        return self.get_gen_of_nrows(self.tl.invoices.list(), nrows)

    def get_gen_of_nrows(self, gen, nrows):
        if nrows < 1:
            return list(gen)
        return [next(gen) for x in range(nrows)]

    @st.cache
    def load_raw_subscriptions_data(self, nrows):
        return self.get_gen_of_nrows(self.tl1.subscriptions.list(), nrows)

    @st.cache
    def load_raw_invoice_data(self, nrows):
        return self.get_gen_of_nrows(self.tl.invoices.list(), nrows)

    @st.cache
    def load_raw_timetracking_data(self, nrows):
        current_date = datetime.today().strftime("%m-%d")
        gen = filter(
            lambda tt: tt["invoiceable"],
            self.tl.timetracking.list(
                data={
                    "filter": {
                        "started_after": "2022-01-01T00:01:49+00:00",
                        "started_before": f"2022-{current_date}T00:01:49+00:00",
                    }
                },
                sideloading="user",
            ),
        )
        return self.get_gen_of_nrows(gen, nrows)

    @st.cache
    def load_raw_tag_data(self, nrows=-1):
        return self.get_gen_of_nrows(self.tl.tags.list(), nrows)

    def get_invoices_subscription(self, subscription):
        r = self.tl1.post_request(
            "getInvoicesBySubscription",
            additional_data={"subscription_id": subscription["id"]},
        )
        return r.json().get("generated_invoice_ids")

    def get_new_invoice_id(self, old_invoice_id):
        try:
            r = self.tl.post_teamleader(
                "migrate.id", data={"type": "invoice", "id": old_invoice_id}
            )
            return r.json()["data"]["id"]
        except KeyError:
            return

    @st.cache(persist=True)
    def load_subscription_related_invoices(self, raw_subscription_data):
        subscription_invoices_old_ids = map(
            self.get_invoices_subscription, raw_subscription_data
        )
        flat_list_subscription_invoices_old_ids = chain(*subscription_invoices_old_ids)
        return list(map(self.get_new_invoice_id, flat_list_subscription_invoices_old_ids))

    def load_invoice_details(self, invoices):
        return [
            self.tl.invoices.info(invoice["id"])
            for _, invoice in invoices[~invoices["Uit abonnement"]].iterrows()
        ]

    def load_tag_company_ids(self, tag):
            return list(
                filter(
                    None,
                    self.tl.companies.list(
                        data={
                            "filter": {"tags": [tag]},
                        }
                    ),
                )
            )


class Dataprocessor:
    def __init__(self, teamleader_client, teamleader_v1_client):
        self.tl1 = teamleader_v1_client
        self.tl = teamleader_client

    def process_raw_invoice_data(
        self, raw_invoice_data, subscription_related_invoices_new_ids
    ):
        invoices_v1 = [
            {
                "id": invoice["id"],
                "customer_id": invoice["invoicee"]["customer"]["id"],
                "factuurdatum": invoice["invoice_date"],
                "betaaldatum": self.parse_date(invoice["paid_at"]),
                "klant": invoice["invoicee"]["name"],
                "totaal": invoice["total"]["tax_exclusive"]["amount"],
                "status": invoice["status"],
                "Uit abonnement": invoice["id"] in subscription_related_invoices_new_ids,
            }
            for invoice in raw_invoice_data
        ]

        invoices_v2 = filter(
            lambda invoice: not (
                    invoice["klant"] == "CollactiveBMK Credit Management"
                    and 2200 < invoice["totaal"] < 5500
            )
                            and invoice["klant"] != "Recht Direct"
                            and invoice["totaal"] != "0.00",
            invoices_v1,
        )

        df = pd.DataFrame(invoices_v2)
        df.betaaldatum.fillna(value=np.nan, inplace=True)

        df["factuurdatum"] = pd.to_datetime(
            df["factuurdatum"], infer_datetime_format=True, errors="coerce"
        )
        df["betaaldatum"] = pd.to_datetime(
            df["betaaldatum"], infer_datetime_format=True, errors="coerce"
        )
        return df

    def process_raw_timetracking_data(self, raw_timetracking_data):
        df_timetracking = pd.DataFrame(
            map(
                lambda tt: {
                    "started_on": tt["started_on"],
                    "duration": tt["duration"] / 60,
                    "description": tt["description"],
                    "user": tt["user"]["first_name"],
                },
                raw_timetracking_data,
            )
        )

        df_timetracking["started_on"] = pd.to_datetime(
            df_timetracking["started_on"], errors="coerce"
        )

        df_timetracking["factuurdatum_jaar"] = df_timetracking["started_on"].dt.year
        df_timetracking["factuurdatum_maand"] = df_timetracking["started_on"].dt.month
        df_timetracking["factuurdatum_week"] = (
            df_timetracking["started_on"].dt.isocalendar().week
        )
        return df_timetracking

    def process_invoice_details(self, raw_invoice_details):
        subscriptions = []
        others = []
        no_company_result = []

        for details in raw_invoice_details:
            for y in details["grouped_lines"]:
                for z in y["line_items"]:
                    if any(
                        [name.lower() in z["description"].lower() for name in ["procit"]]
                    ):
                        no_company_result.append(details)
                    elif not any(
                        [
                            x in z["description"].lower()
                            for x in [
                                "echt ontzorgd",
                                "abonnement",
                                "digitaal ontzorgd",
                                "volledig digitaal",
                                "écht ontzorgd +",
                            ]
                        ]
                    ):
                        others.append(details)
                    else:
                        subscriptions.append(details)
        return subscriptions, others, no_company_result


    def parse_date(self, d):
        if d:
            return dateparser.parse(d).strftime("%Y-%m-%d")
        return d

    def process_raw_subscription_data(self, raw_subscription_data):
        df = pd.DataFrame(raw_subscription_data)
        for colum_name in [
            "date_start_formatted",
            "date_end_formatted",
            "next_renewal_date_formatted",
        ]:
            df[colum_name] = pd.to_datetime(
                df[colum_name],
                format="%d/%m/%Y",
                errors="coerce",
                infer_datetime_format=True,
            )

        df["end_date_or_renewal"] = df["date_end_formatted"].fillna(
            df["next_renewal_date_formatted"]
        )

        df = df[
            [
                "active",
                "title",
                "repeat",
                "client_name",
                "department_name",
                "date_start_formatted",
                "date_end_formatted",
                "end_date_or_renewal",
                "total_price_excl_vat",
                "next_renewal_date_formatted",
                "contact_or_company",
            ]
        ]
        return df


def update_invoices_with_details(invoices, subscriptions_detailed_invoices):
    ids = set(
        [invoice_details["id"] for invoice_details in subscriptions_detailed_invoices]
    )
    for idx, row in invoices.iterrows():
        if row["id"] in ids:
            invoices.loc[idx, "Uit abonnement"] = True


def get_details_ids(details):
    return [invoice_details["id"] for invoice_details in details]


def load_excluded_ids():
    if "invoice_ids_to_be_excluded" not in st.session_state:
        return []
    return st.session_state['invoice_ids_to_be_excluded']


def get_selected_ids_to_be_excluded(no_company_result_detailed_invoices):
    return set(
        [
            "3ae003b8-cce6-0389-ab60-db361ac1e046",
            "777a0bd9-125b-0853-b06d-549dbbe286c5",
            "57b82535-7a74-007a-8566-6c343bb2d30e",
            "07b2eaf9-2121-010d-9a6a-3c1f01e836a0",
            "4b0694a3-11ea-073c-8467-accd7eace214",
            "21869bdc-b1eb-0442-a263-b128c2ddc9a1",
        ]
        + load_excluded_ids()
        + [invoice_details["id"] for invoice_details in no_company_result_detailed_invoices]
    )


def save_excluded_ids(options):
    st.session_state['invoice_ids_to_be_excluded'] = options


def delete_invoice_rows(invoices, to_be_deleted_ids):
    for id in to_be_deleted_ids:
        invoices = invoices[invoices["id"] != id]

    return invoices


def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + timedelta(n)


def get_date_list(start_dt, end_dt):
    return [dt for dt in daterange(start_dt, end_dt)]


def get_aantal_abonnementen(df_subscriptions):
    counter = {}
    for idx, subs in df_subscriptions.iterrows():
        start_date = subs["date_start_formatted"]
        end_date = subs["end_date_or_renewal"]

        for day in get_date_list(start_date, end_date):
            counter[day] = counter.get(day, 0) + 1
    st.text(
        f"Het aantal abonnementen vandaag ({datetime.today().strftime('%d-%m-%Y')}) is:"
        f" {counter[pd.Timestamp(datetime.today().strftime('%Y-%m-%d'))]}"
    )

    res = pd.DataFrame.from_dict(counter, orient="index").reset_index()

    res.columns = ["Datum", "Aantal abonnementen"]

    res["Datum"] = pd.to_datetime(res["Datum"], errors="coerce")

    a = res["Datum"] > datetime(2020, 1, 1)

    b = res["Datum"] < datetime.now() + timedelta(days=90)

    aa = res["Datum"] > datetime.now() - timedelta(days=30)

    bb = res["Datum"] < datetime.now() + timedelta(days=30)
    plusmin30days = res[aa & bb]

    res = res[a & b]

    return res.set_index("Datum"), plusmin30days.set_index("Datum")


def run_statistics(tl, teamleader_v1_client):
    st.title("Cijfers Recht Direct")
    dl = Dataloader(tl, teamleader_v1_client)
    nrows = int(st.number_input("Hoeveel rijen wil je? (-1 voor alles)", value=10, step=10))
    # Create a text element and let the reader know the data is loading.
    data_load_state = st.text("Loading data...")
    # Loading raw data
    data_load_state.text("Loading raw subscription data...")
    raw_subscription_data = dl.load_raw_subscriptions_data(nrows)
    data_load_state.text("Loading raw invoice data...")
    raw_invoice_data = dl.load_raw_invoice_data(nrows)
    data_load_state.text("Loading raw timetracking data...")
    raw_timetracking_data = dl.load_raw_timetracking_data(nrows)
    data_load_state.text("Loading raw tag data...")
    raw_tag_data = dl.load_raw_tag_data()

    # Process data
    data_load_state.text("Processing raw subscription data...")
    dp = Dataprocessor(tl, teamleader_v1_client)
    subscriptions = dp.process_raw_subscription_data(raw_subscription_data)
    data_load_state.text("Loading subscription related invoice data...")
    subscription_related_invoices_new_ids = dl.load_subscription_related_invoices(
        raw_subscription_data
    )
    data_load_state.text("Processing raw invoice data...")
    invoices = dp.process_raw_invoice_data(
        raw_invoice_data, subscription_related_invoices_new_ids
    )

    data_load_state.text("Processing raw timetracking data...")
    timetracking = dp.process_raw_timetracking_data(raw_timetracking_data)
    # Notify the reader that the data was successfully loaded.
    data_load_state.text("Loading details of non subscriptions invoices")
    raw_invoice_details = dl.load_invoice_details(invoices)
    data_load_state.text("Processing details of non subscriptions invoices")
    (
        subscriptions_detailed_invoices,
        other_invoices_ids_detailed_invoices,
        no_company_result_detailed_invoices,
    ) = dp.process_invoice_details(raw_invoice_details)
    data_load_state.text("Updating invoice details into general invoice data")
    update_invoices_with_details(invoices, subscriptions_detailed_invoices)
    options = st.sidebar.multiselect(
        label="De factuur ID's die niet moeten worden meegenomen",
        options=get_details_ids(raw_invoice_data)
        + list(get_selected_ids_to_be_excluded(no_company_result_detailed_invoices)),
        default=get_selected_ids_to_be_excluded(no_company_result_detailed_invoices),
    )
    save_excluded_ids(options)
    invoices = delete_invoice_rows(invoices, options)
    data_load_state.text("Processing raw tag data...")
    tags = list(map(lambda tag: tag["tag"], raw_tag_data))
    data_load_state.text("Loading done!")

    tags_doorverwijzers = st.multiselect(
        label="De doorverwijzings tags die we willen weten:",
        options=tags,
        default=[],
    )
    st.write(tags_doorverwijzers)
    st.write(invoices)
    doorverwijzers = dict(
        map(lambda tag: (tag, dl.load_tag_company_ids(tag)), tags_doorverwijzers)
    )

    if st.checkbox("Show subscription data"):
        st.subheader("Subscription data")
        st.write(subscriptions.style.format(precision=2))

    if st.checkbox("Show invoice data"):
        st.subheader("Invoice data")
        st.write(invoices.style.format(precision=2))

    if st.checkbox("Show timetracking data"):
        st.subheader("Timetracking data")
        st.write(timetracking.style.format(precision=2))

    st.subheader("Aantal abonnementen")
    aantal_abonnementen, kortetermijn_aantal_abonnementen = get_aantal_abonnementen(
        subscriptions
    )
    st.bar_chart(aantal_abonnementen)
    st.bar_chart(kortetermijn_aantal_abonnementen)


    st.subheader("Projecturen per week 2022")
    st.dataframe(timetracking)
    st.dataframe(
        timetracking[["factuurdatum_week", "duration"]]
            .groupby("factuurdatum_week")
            .sum()
            .style.format(precision=1)
    )

