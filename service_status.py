from flask import Flask
app = Flask(__name__)


@app.route("/")
def home_page():
    return "Hello!<br>Available methods:" \
           "<br>/service_status/get_records_by_ip/ip" \
           "<br>/service_status/get_records/ip/port"


@app.route('/service_status/get_records_by_ip/<string:ip>')
def get_records_by_ip(ip):
    return ip


@app.route('/service_status/get_records/<string:ip>/<int:port>')
def get_records(ip, port):
    return ip


if __name__ == '__main__':
    app.run(debug=True)
