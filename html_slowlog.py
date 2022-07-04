# /bin/python
# coding=utf-8

def html_fomat(d1,d2):
    html_body = """
        <html><head>
    <meta charset="UTF-8">
    <style>
    .mytable table {
        width:100%%;
        margin:15px 0;
        border:0;
    }
    .mytable,.mytable th,.mytable td {
        font-size:0.9em;
        text-align:left;
        padding:4px;
        border-collapse:collapse;
    }
    .mytable th,.mytable td {
        border: 1px solid #ffffff;
        border-width:1px
    }
    .mytable th {
        border: 1px solid #cde6fe;
        border-width:1px 0 1px 0
    }
    .mytable td {
        border: 1px solid #eeeeee;
        border-width:1px 0 1px 0
    }
    .mytable tr {
        border: 1px solid #ffffff;
    }
    .mytable tr:nth-child(odd){
        background-color:#f7f7f7;
    }
    .mytable tr:nth-child(even){
        background-color:#ffffff;
    }
    .mytable2 th, .mytable2 td {
        border-width:1px 1 1px 1
    }
    </style>
        </head><body>
            <div>
            <h2>SLOWLOG INFO(生产环境慢查询):</h2>
                <table class='mytable'>
                  <tr>
                    <th>Execution_count</th>
                    <th>cst_ExecutionStartTime</th>
                    <th>avg_quertime</th>
                    <th>max_quertime</th>
                    <th>fingerprint</th>
                    <th>HostAddress</th>
                    <th>Instan_Name</th>
                    <th>db_Name</th>
                    <th>ParseRowCounts</th>
                    <th>ReturnRowCounts</th>
                    <th>sqltext</th>
                    
                  </tr>
                  %s
                </table>
            </div><br/>
            
            </div><br/>

            <div>
            <h2>FULL SCAN SQL(全表扫描SQL):</h2>
            <table class='mytable'>
              <tr>
                <th>Execution_count</th>
                    <th>cst_ExecutionStartTime</th>
                    <th>avg_quertime</th>
                    <th>max_quertime</th>
                    <th>fingerprint</th>
                    <th>HostAddress</th>
                    <th>Instan_Name</th>
                    <th>db_Name</th>
                    <th>ParseRowCounts</th>
                    <th>ReturnRowCounts</th>
                    <th>sqltext</th>
              </tr>
              %s
            </table>
        </div><br/>


    </body></html>
    """ % ("\n".join(map(str, d1)),"\n".join(map(str, d2)))
    return html_body
