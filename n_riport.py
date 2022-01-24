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
        self._previous_live_file = ""
        self.body = ""
        self.run_time_start = None
        self.run_time_stop = None
        self.length = 120
        self.live_text = ""

    def stamp_live(self):
        if self._previous_live_file != "":
            os.remove(self._previous_live_file)
        i_file_name = "LIVE_arb_at_"+self.get_dt_tag() + ".txt"

        with open(i_file_name, "w", encoding="utf-8") as f:
            f.write(self.live_text)
        f.close()
        self._previous_live_file = i_file_name

    def get_dt_tag(self):
        i_dt = datetime.datetime.now()
        i_mo = "00" + str(i_dt.month)
        i_da = "00" + str(i_dt.day)
        i_h = "00" + str(i_dt.hour+1)
        i_m = "00" + str(i_dt.minute)
        i_s = "00" + str(i_dt.second)
        return i_mo[-2:] + i_da[-2:] + "_" + i_h[-2:] + i_m[-2:] + i_s[-2:]

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
        self.add("-" * self.length)

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
    riport.create("C1_RIpORT", "Arbitrazs gyakorisagokat vizsgal")

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

