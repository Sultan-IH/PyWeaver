from psycopg2.pool import ThreadedConnectionPool
import yaml


class DBCLient:
    def __init__(self, file_path, conn=None):
        if file_path:
            with open(file_path) as file:
                self.config = yaml.load(file)
            conn_params = (
                f"dbname={self.config['DB_NAME']} user={self.config['DB_USER']}"
                f" password={self.config['DB_PASSWORD']} host={self.config['DB_HOST']} ")
            pool = ThreadedConnectionPool(1, 40, conn_params)

            self.pool = pool

        elif conn:
            conn.autocommit = True
            self.cursor = conn.cursor()

    def new_cli(self):
        new_conn = self.pool.getconn()
        return DBCLient(file_path=None, conn=new_conn)

    def update_score_comment(self, comment_id, score):
        sql_statement = f"""
                UPDATE reddit_replies
                SET ups = {score}
                WHERE id = '{comment_id}'
            """
        self.cursor.execute(sql_statement)
