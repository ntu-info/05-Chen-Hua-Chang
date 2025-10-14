# app.py
from flask import Flask, jsonify, send_file
import os
from sqlalchemy import create_engine, text

_engine = None

# -----------------------------
# Database Connection
# -----------------------------
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


# -----------------------------
# Flask App
# -----------------------------
def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    # ----------------------------------------
    # 1️⃣ Dissociate by TERMS (A \ B)
    # ----------------------------------------
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="terms_dissociate")
    def dissociate_terms(term_a, term_b):
        eng = get_engine()
        payload = {"ok": False, "term_a": term_a, "term_b": term_b, "studies": []}
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                sql = text("""
                    SELECT DISTINCT a.study_id
                    FROM ns.annotations_terms a
                    WHERE LOWER(a.term) = LOWER(:term_a)
                      AND a.study_id NOT IN (
                          SELECT study_id FROM ns.annotations_terms WHERE LOWER(term) = LOWER(:term_b)
                      )
                    LIMIT 50
                """)
                rows = conn.execute(sql, {"term_a": term_a, "term_b": term_b}).all()
                payload["studies"] = [r[0] for r in rows]
            payload["ok"] = True
            return jsonify(payload), 200
        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500


    # ----------------------------------------
    # 2️⃣ Dissociate by COORDINATES (A \ B)
    # ----------------------------------------
    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="locations_dissociate")
    def dissociate_locations(coords_a, coords_b):
        eng = get_engine()
        payload = {"ok": False, "coords_a": coords_a, "coords_b": coords_b, "studies": []}
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                x1, y1, z1 = map(float, coords_a.split("_"))
                x2, y2, z2 = map(float, coords_b.split("_"))

                # 用 ST_DWithin 避免浮點比較誤差
                sql = text("""
                    SELECT DISTINCT c1.study_id
                    FROM ns.coordinates c1
                    WHERE ST_DWithin(c1.geom, ST_MakePoint(:x1, :y1, :z1)::geometry, 2)
                      AND c1.study_id NOT IN (
                          SELECT study_id FROM ns.coordinates
                          WHERE ST_DWithin(geom, ST_MakePoint(:x2, :y2, :z2)::geometry, 2)
                      )
                    LIMIT 50
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


# -----------------------------
# WSGI Entry Point
# -----------------------------
app = create_app()
