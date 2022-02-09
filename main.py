from DBUpdate import *
from groundObs2Pg import *
from data_filtering import *
from data_filtering_spa import *

# Get files
time_diff = 1
out_path = r"C:\Users\hsaf2\Desktop\DBupdate\Data"
endtime = GetFiles(time_diff, out_path)

# Import files

dat = DataMigration()
dat.process_path = out_path
dat.end_time = endtime

r = datetime.datetime.strptime(endtime, "%Y%m%d") - datetime.datetime.today()
for i in range(abs(r.days)):
    c = datetime.datetime.strptime(endtime, "%Y%m%d") + datetime.timedelta(days=i)
    date_ = c.strftime("%Y%m%d")
    dat.read_and_write_files(date_)

# Inset to DB
dat.InserInto()

# Update Awos
end_date = datetime.datetime.strptime(endtime, '%Y%m%d').strftime('%Y-%m-%d')
filterAll(end_date)

# Update SPA
filterSPA(end_date)

# Update MatViews

dat.refrehDB()
