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

    # -----------------------
    # Health check
    # -----------------------
    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    # -----------------------
    # Show test image
    # -----------------------
    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    # -----------------------
    # Helper functions
    # -----------------------
    def query_terms(term_a, term_b):
        """Return studies that contain term_a but not term_b using ILIKE + DISTINCT"""
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            sql = text("""
                SELECT DISTINCT a.study_id
                FROM annotations_terms a
                WHERE a.term ILIKE :term_a
                  AND a.study_id NOT IN (
                      SELECT study_id FROM annotations_terms WHERE term ILIKE :term_b
                  )
            """)
            rows = conn.execute(sql, {"term_a": term_a, "term_b": term_b}).all()
            return [r[0] for r in rows]

    def query_coords(coords_a, coords_b):
        """Return studies that contain coords_a but not coords_b (保持原本能跑的方式)"""
        x1, y1, z1 = map(float, coords_a.split("_"))
        x2, y2, z2 = map(float, coords_b.split("_"))
        eng = get_engine()
        with eng.begin() as conn:
            conn.execute(text("SET search_path TO ns, public;"))
            sql = text("""
                SELECT DISTINCT c1.study_id
                FROM coordinates c1
                WHERE c1.x = :x1
                  AND c1.y = :y1
                  AND c1.z = :z1
                  AND c1.study_id NOT IN (
                      SELECT study_id FROM coordinates
                      WHERE x = :x2
                        AND y = :y2
                        AND z = :z2
                  )
            """)
            rows = conn.execute(sql, {"x1": x1, "y1": y1, "z1": z1,
                                      "x2": x2, "y2": y2, "z2": z2}).all()
            return [r[0] for r in rows]

    # -----------------------
    # Dissociate by terms
    # -----------------------
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="terms_dissociate")
    def dissociate_terms(term_a, term_b):
        try:
            studies = query_terms(term_a, term_b)
            return jsonify(studies), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/dissociate/terms/<term1>/<term2>/both", endpoint="terms_dissociate_both")
    def dissociate_terms_both(term1, term2):
        try:
            result = {
                "A_minus_B": query_terms(term1, term2),
                "B_minus_A": query_terms(term2, term1)
            }
            return jsonify(result), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # -----------------------
    # Dissociate by coordinates
    # -----------------------
    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="locations_dissociate")
    def dissociate_locations(coords_a, coords_b):
        try:
            studies = query_coords(coords_a, coords_b)
            return jsonify(studies), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/dissociate/locations/<coords1>/<coords2>/both", endpoint="locations_dissociate_both")
    def dissociate_locations_both(coords1, coords2):
        try:
            result = {
                "A_minus_B": query_coords(coords1, coords2),
                "B_minus_A": query_coords(coords2, coords1)
            }
            return jsonify(result), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # -----------------------
    # Test DB connection
    # -----------------------
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
            return jsonify(payload), 200
        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app


# WSGI entry point
app = create_app()
