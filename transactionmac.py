import datetime
from datetime import timedelta
from enum import Enum
import os
import psycopg
import random
import subprocess
import time
import uuid

class Field(Enum):
    ssn = 0
    cc_num = 1
    first = 2
    last = 3
    gender = 4
    street = 5
    city = 6
    state = 7
    zip = 8
    lat = 9
    lng = 10
    city_pop = 11
    job = 12
    dob = 13
    acct_num = 14
    profile = 15
    trans_num = 16
    trans_date = 17
    trans_time = 18
    unix_time = 19
    category = 20
    amt = 21
    is_fraud = 22
    merchant = 23
    merch_lat = 24
    merch_lng = 25
    merch_id = 26


class Transactionmac:

    def __init__(self, args: dict):
        # args is a dict of string passed with the --args flag
        # user passed a yaml/json, in python that's a dict object
        self.customers: int = int(args.get("customers", 10))
        self.days: int = int(args.get("days", 10))
        self.batch_size: int = int(args.get("batch_size", 128))
        self.update_freq: int = int(args.get("update_freq", 10))
        self.generator_location: string = str(args.get("generator_location",
            f"{os.environ['HOME']}/workspace/example-ml-flow/Sparkov_Data_Generation"))
        self.data_folder: string = str(args.get("data_folder",
            f"{os.environ['HOME']}/workspace/example-ml-flow/data/generated"))

        # you can arbitrarely add any variables you want
        self.counter: int = 0



    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        self.id = id
        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total count of threads is {total_thread_count}"
            )
            print(cur.execute(f"select version()").fetchone()[0])



    # the run() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, run() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):
        command = [
            "rm",
            "-rf",
            f"{self.data_folder}/{self.id}"
        ]
        # print(f"executing command: {command}")
        subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        start_days_ahead = (self.days * self.counter) + 1
        start_date=datetime.datetime.now() + timedelta(days=start_days_ahead)
        end_date=start_date + timedelta(days=self.days)

        command = [
            "python3",
            "./datagen.py",
            "-n",
            str(self.customers),
            "-o",
            f"{self.data_folder}/{self.id}",
            start_date.strftime("%m-%d-%Y"),
            end_date.strftime("%m-%d-%Y")
        ]
        # print(f"executing command: {command}")
        subprocess.run(command, cwd=self.generator_location,
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

        self.records = []
        # print(f"id: {self.id} and counter: {self.counter} LOOP completed")
        return [self.parse, self.address, self.city_loc, self.customer, self.merchant, self.transaction]



    # conn is an instance of a psycopg connection object
    # conn is set by default with autocommit=True, so no need to send a commit message
    def parse(self, conn: psycopg.Connection):
        self.counter += 1
        # print(f"id: {self.id} and counter: {self.counter} PARSE called")

        directory = f"{self.data_folder}/{self.id}"
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            base = os.path.basename(filepath)
            ext = os.path.splitext(filepath)[-1]
            if not base.startswith('customers') and ext == '.csv':
                with open(filepath, 'r') as file:
                    next(file) # skip the first line (header)
                    for line in file:
                        record = line.strip().split('|')
                        record.append(uuid.uuid5(uuid.NAMESPACE_DNS, record[Field.merchant.value]))
                        self.records.append(record)



    def execute(self, conn: psycopg.Connection, data, record_cnt, ins_sql, con_sql):
        fields = ','.join("%s" for i in range(int(len(data) / record_cnt)))
        values = ','.join(f"({fields})" for i in range(record_cnt))
        with conn.cursor() as cur:
            cur.execute(f"{ins_sql} VALUES {values} {con_sql};", tuple(data))



    def address(self, conn: psycopg.Connection):
        # print(f"id: {self.id} and counter: {self.counter} ADDRESS called")

        sql = """
        INSERT INTO address (
            acct_num, street, zip, lat, lng
        )
        """

        record_cnt = 0
        data = []
        unique_list = []
        for record in self.records:
            acct_num = record[Field.acct_num.value]
            if acct_num in unique_list:
                continue
            else:
                unique_list.append(acct_num)

            record_cnt += 1
            data += [
                acct_num,
                record[Field.street.value],
                record[Field.zip.value],
                record[Field.lat.value],
                record[Field.lng.value]
            ]

            if record_cnt >= self.batch_size:
                if (random.randint(1, 100) <= self.update_freq):
                    resolve_it = """
                    ON CONFLICT (acct_num) DO UPDATE SET
                        street = excluded.street,
                        zip = excluded.zip,
                        lat = excluded.lat,
                        lng = excluded.lng
                    """
                    self.execute(conn, data, record_cnt, sql, resolve_it)
                else:
                    do_nothing = "ON CONFLICT DO NOTHING"
                    self.execute(conn, data, record_cnt, sql, do_nothing)
                record_cnt = 0
                data = []
                unique_list = []

        if record_cnt > 0:
            if (random.randint(1, 100) <= self.update_freq):
                resolve_it = """
                ON CONFLICT (acct_num) DO UPDATE SET
                    street = excluded.street,
                    zip = excluded.zip,
                    lat = excluded.lat,
                    lng = excluded.lng
                """
                self.execute(conn, data, record_cnt, sql, resolve_it)
            else:
                do_nothing = "ON CONFLICT DO NOTHING"
                self.execute(conn, data, record_cnt, sql, do_nothing)



    def city_loc(self, conn: psycopg.Connection):
        # print(f"id: {self.id} and counter: {self.counter} CITY_LOC called")

        sql = """
        INSERT INTO city_loc (
            zip, city, state, city_pop
        )
        """

        record_cnt = 0
        data = []
        unique_list = []
        for record in self.records:
            zip = record[Field.zip.value]
            if zip in unique_list:
                continue
            else:
                unique_list.append(zip)

            record_cnt += 1
            data += [
                zip,
                record[Field.city.value],
                record[Field.state.value],
                record[Field.city_pop.value]
            ]

            if record_cnt >= self.batch_size:
                if (random.randint(1, 100) <= self.update_freq):
                    resolve_it = """
                    ON CONFLICT (zip) DO UPDATE SET
                        city = excluded.city,
                        state = excluded.state,
                        city_pop = excluded.city_pop
                    """
                    self.execute(conn, data, record_cnt, sql, resolve_it)
                else:
                    do_nothing = "ON CONFLICT DO NOTHING"
                    self.execute(conn, data, record_cnt, sql, do_nothing)
                record_cnt = 0
                data = []
                unique_list = []

        if record_cnt > 0:
            if (random.randint(1, 100) <= self.update_freq):
                resolve_it = """
                ON CONFLICT (zip) DO UPDATE SET
                    city = excluded.city,
                    state = excluded.state,
                    city_pop = excluded.city_pop
                """
                self.execute(conn, data, record_cnt, sql, resolve_it)
            else:
                do_nothing = "ON CONFLICT DO NOTHING"
                self.execute(conn, data, record_cnt, sql, do_nothing)



    def customer(self, conn: psycopg.Connection):
        # print(f"id: {self.id} and counter: {self.counter} CUSTOMER called")

        sql = """
        INSERT INTO customer (
            ssn, cc_num, first, last, gender, job, dob, acct_num, profile
        )
        """

        record_cnt = 0
        data = []
        unique_list = []
        for record in self.records:
            ssn = record[Field.ssn.value]
            if ssn in unique_list:
                continue
            else:
                unique_list.append(ssn)

            record_cnt += 1
            data += [
                ssn,
                record[Field.cc_num.value],
                record[Field.first.value],
                record[Field.last.value],
                record[Field.gender.value],
                record[Field.job.value],
                record[Field.dob.value],
                record[Field.acct_num.value],
                record[Field.profile.value]
            ]

            if record_cnt >= self.batch_size:
                if (random.randint(1, 100) <= self.update_freq):
                    resolve_it = """
                    ON CONFLICT (ssn) DO UPDATE SET
                        cc_num = excluded.cc_num,
                        first = excluded.first,
                        last = excluded.last,
                        gender = excluded.gender,
                        job = excluded.job,
                        dob = excluded.dob,
                        acct_num = excluded.acct_num,
                        profile = excluded.profile
                    """
                    self.execute(conn, data, record_cnt, sql, resolve_it)
                else:
                    do_nothing = "ON CONFLICT DO NOTHING"
                    self.execute(conn, data, record_cnt, sql, do_nothing)
                record_cnt = 0
                data = []
                unique_list = []

        if record_cnt > 0:
            if (random.randint(1, 100) <= self.update_freq):
                resolve_it = """
                ON CONFLICT (ssn) DO UPDATE SET
                    cc_num = excluded.cc_num,
                    first = excluded.first,
                    last = excluded.last,
                    gender = excluded.gender,
                    job = excluded.job,
                    dob = excluded.dob,
                    acct_num = excluded.acct_num,
                    profile = excluded.profile
                """
                self.execute(conn, data, record_cnt, sql, resolve_it)
            else:
                do_nothing = "ON CONFLICT DO NOTHING"
                self.execute(conn, data, record_cnt, sql, do_nothing)



    def merchant(self, conn: psycopg.Connection):
        # print(f"id: {self.id} and counter: {self.counter} MERCHANT called")

        sql = """
        INSERT INTO merchant (
            id, merchant, merch_lat, merch_lng
        )
        """

        record_cnt = 0
        data = []
        unique_list = []
        for record in self.records:
            id = record[Field.merch_id.value]
            if id in unique_list:
                continue
            else:
                unique_list.append(id)

            record_cnt += 1
            data += [
                id,
                record[Field.merchant.value],
                record[Field.merch_lat.value],
                record[Field.merch_lng.value]
            ]

            if record_cnt >= self.batch_size:
                if (random.randint(1, 100) <= self.update_freq):
                    resolve_it = """
                    ON CONFLICT (id) DO UPDATE SET
                        merchant = excluded.merchant,
                        merch_lat = excluded.merch_lat,
                        merch_lng = excluded.merch_lng
                    """
                    self.execute(conn, data, record_cnt, sql, resolve_it)
                else:
                    do_nothing = "ON CONFLICT DO NOTHING"
                    self.execute(conn, data, record_cnt, sql, do_nothing)
                record_cnt = 0
                data = []
                unique_list = []

        if record_cnt > 0:
            if (random.randint(1, 100) <= self.update_freq):
                resolve_it = """
                ON CONFLICT (id) DO UPDATE SET
                    merchant = excluded.merchant,
                    merch_lat = excluded.merch_lat,
                    merch_lng = excluded.merch_lng
                """
                self.execute(conn, data, record_cnt, sql, resolve_it)
            else:
                do_nothing = "ON CONFLICT DO NOTHING"
                self.execute(conn, data, record_cnt, sql, do_nothing)



    def transaction(self, conn: psycopg.Connection):
        # print(f"id: {self.id} and counter: {self.counter} TRANSACTION called")

        sql = """
        INSERT INTO transaction (
            cc_num, trans_num, trans_date, trans_time, unix_time, category, merch_id, amt, is_fraud
        )
        """

        record_cnt = 0
        data = []
        for record in self.records:
            record_cnt += 1
            data += [
                record[Field.cc_num.value],
                record[Field.trans_num.value],
                record[Field.trans_date.value],
                record[Field.trans_time.value],
                record[Field.unix_time.value],
                record[Field.category.value],
                record[Field.merch_id.value],
                record[Field.amt.value],
                record[Field.is_fraud.value]
            ]

            if record_cnt >= self.batch_size:
                self.execute(conn, data, record_cnt, sql, "")
                record_cnt = 0
                data = []
                unique_list = []

        if record_cnt > 0:
            self.execute(conn, data, record_cnt, sql, "")
