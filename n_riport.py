import datetime
import textwrap
import os
import time


class n_riport():
    def __init__(self):
        self.name = ""
        self.description = ""
        self.delete_previous = True
        self.previous_file = ""
        self.body = ""
        self.run_time_start = None
        self.run_time_stop = None
        self.length = 100

    def get_dt_tag(self):
        i_dt = datetime.datetime.now()
        return str(i_dt.year) + str(i_dt.month) + str(i_dt.day) + "_" + str(i_dt.hour) + str(i_dt.minute) + str(
            i_dt.second)

    def get_file_name(self):
        return str(self.name) + "_" + self.get_dt_tag()

    def create(self, name, description):
        self.stamp_runtime("start")
        self.name = str(name).upper()
        self.description = description
        self.body = ""
        self.add("Riport name:", self.name)
        self.add(self.description)
        self.add_line()

    def clear(self):
        self.body = ""
        self.add("Riport name:", self.name)
        self.add(self.description)
        self.add_line()

    def add(self, element1="", element2="", element3="", element4=""):
        i_add = str(element1) + " " + str(element2) + " " + str(element3) + " " + str(element4)
        i_add = textwrap.fill(i_add, width=self.length)
        self.body += i_add + "\n"

    def show(self):
        self.stamp_runtime("stop")
        self.add_runtime()
        print(self.body)

    def write(self):
        self.stamp_runtime("stop")
        self.add_runtime()
        if self.delete_previous:
            if self.previous_file != "":
                os.remove(self.previous_file)
        i_file_name = self.get_file_name() + ".txt"

        with open(i_file_name, "w", encoding="utf-8") as f:
            f.write(self.body)
        f.close()
        self.previous_file = i_file_name

    def add_line(self):
        self.add("─" * self.length)

    def stamp_runtime(self, pos):
        if pos == "start" or pos == "START":
            self.run_time_start = str(datetime.datetime.now())
        else:
            self.run_time_stop = str(datetime.datetime.now())

    def add_runtime(self):
        self.add_line()
        self.add("Runtime:", self.run_time_start, "-", self.run_time_stop)

    def add_section(self, section_name):
        self.add("\n")
        i_add = str(section_name) + " " + "." * self.length
        self.add(i_add[:self.length])


if __name__ == '__main__':

    x1 = []
    for i in range(20):
        x1.append(str(i))

    riport = n_riport()
    riport.create("C1_RIpORT", "Arbitrázs gyakoriságokat vizsgál")

    riport.add("Tömb: ", x1)
    riport.add("Tömb2: ", x1)
    riport.add_section("harmadik")
    riport.add("hello")

    riport.write()

    time.sleep(5)

    riport.clear()
    riport.add("Tömb: ", x1)
    riport.add("Tömb2: ", x1)
    riport.add_section("harmadik")
    riport.add("hello")

    riport.show()
    riport.write()

