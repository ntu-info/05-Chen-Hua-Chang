# app.py
from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(db_url, pool_pre_ping=True)
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    # --- Dissociate by terms ---
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="terms_dissociate")
    def dissociate_terms(term_a, term_b):
        eng = get_engine()
        payload = {"ok": False, "term_a": term_a, "term_b": term_b, "studies": []}
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                # 查出包含 term_a 但不包含 term_b 的 study_id
                sql = text("""
                    SELECT DISTINCT a.study_id
                    FROM annotations_terms a
                    WHERE LOWER(a.term) = LOWER(:term_a)
                    AND a.study_id NOT IN (
                        SELECT study_id FROM annotations_terms WHERE LOWER(term) = LOWER(:term_b)
                    )
                    LIMIT 20
                """)
                rows = conn.execute(sql, {"term_a": term_a, "term_b": term_b}).all()
                payload["studies"] = [r[0] for r in rows]
            payload["ok"] = True
            return jsonify(payload), 200
        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    # --- Dissociate by coordinates ---
    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="locations_dissociate")
    def dissociate_locations(coords_a, coords_b):
        eng = get_engine()
        payload = {"ok": False, "coords_a": coords_a, "coords_b": coords_b, "studies": []}
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                x1, y1, z1 = map(float, coords_a.split("_"))
                x2, y2, z2 = map(float, coords_b.split("_"))

                # 查出包含 coords_a 但不包含 coords_b 的 study_id
                sql = text("""
                    SELECT DISTINCT c1.study_id
                    FROM coordinates c1
                    WHERE ST_X(c1.geom) = :x1
                      AND ST_Y(c1.geom) = :y1
                      AND ST_Z(c1.geom) = :z1
                      AND c1.study_id NOT IN (
                          SELECT study_id FROM coordinates
                          WHERE ST_X(geom) = :x2
                            AND ST_Y(geom) = :y2
                            AND ST_Z(geom) = :z2
                      )
                    LIMIT 20
                """)
                rows = conn.execute(sql, {
                    "x1": x1, "y1": y1, "z1": z1,
                    "x2": x2, "y2": y2, "z2": z2
                }).all()
                payload["studies"] = [r[0] for r in rows]
            payload["ok"] = True
            return jsonify(payload), 200
        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app

# WSGI entry point
app = create_app()
