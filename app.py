# app.py
from flask import Flask, jsonify, send_file
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

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()
            payload["ok"] = True
            return jsonify(payload)
        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    # Dissociate by terms: studies that mention term_a but NOT term_b
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_terms(term_a, term_b):
        eng = get_engine()
        result = {"ok": False, "term_a": term_a, "term_b": term_b, "studies": []}
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                # A \ B: select study_id that has term_a but not term_b
                query = text("""
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term = :term_a
                      AND study_id NOT IN (
                          SELECT study_id FROM ns.annotations_terms WHERE term = :term_b
                      )
                    LIMIT 100
                """)
                rows = conn.execute(query, {"term_a": term_a, "term_b": term_b}).scalars().all()
                result["studies"] = rows
                result["ok"] = True
            return jsonify(result)
        except Exception as e:
            result["error"] = str(e)
            return jsonify(result), 500

    # Dissociate by coordinates: studies that mention coords1 but NOT coords2
    @app.get("/dissociate/locations/<coords1>/<coords2>", endpoint="dissociate_locations")
    def dissociate_locations(coords1, coords2):
        eng = get_engine()
        result = {"ok": False, "coords1": coords1, "coords2": coords2, "studies": []}
        try:
            x1, y1, z1 = map(int, coords1.split("_"))
            x2, y2, z2 = map(int, coords2.split("_"))
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                # find studies containing coords1
                query1 = text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x1 AND ST_Y(geom) = :y1 AND ST_Z(geom) = :z1
                """)
                studies1 = set(conn.execute(query1, {"x1": x1, "y1": y1, "z1": z1}).scalars().all())
                # find studies containing coords2
                query2 = text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x2 AND ST_Y(geom) = :y2 AND ST_Z(geom) = :z2
                """)
                studies2 = set(conn.execute(query2, {"x2": x2, "y2": y2, "z2": z2}).scalars().all())
                # A \ B: in coords1 but not coords2
                result["studies"] = list(studies1 - studies2)
                result["ok"] = True
            return jsonify(result)
        except Exception as e:
            result["error"] = str(e)
            return jsonify(result), 500

    return app

# WSGI entry point
app = create_app()
