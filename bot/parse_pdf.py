"""Parse questions from Google Forms results PDF."""
import re
import json
import sys
import pdfplumber

CORRECT_COLOR_G = 0.5569

# Section start pages (0-indexed, skipping intro page 0)
SECTIONS = {
    1:  "КОНСТИТУЦІЙНЕ ПРАВО",
    9:  "АДМІНІСТРАТИВНЕ ПРАВО",
    22: "ЦИВІЛЬНЕ ПРАВО",
    33: "ЦИВІЛЬНЕ ПРОЦЕСУАЛЬНЕ ПРАВО",
    43: "КРИМІНАЛЬНЕ ПРАВО",
    52: "КРИМІНАЛЬНО-ПРОЦЕСУАЛЬНЕ ПРАВО",
    61: "МІЖНАРОДНЕ ПУБЛІЧНЕ ПРАВО",
    70: "МІЖНАРОДНИЙ ЗАХИСТ ПРАВ ЛЮДИНИ",
}


def get_section(page_idx: int) -> str:
    current = "ЗАГАЛЬНЕ"
    for p, name in sorted(SECTIONS.items()):
        if page_idx >= p:
            current = name
    return current


def is_inner_circle(c) -> bool:
    return 3 < c["width"] < 5 and 3 < c["height"] < 5


def clean_question_text(text: str) -> str:
    text = re.sub(r"\s+\d+\s*$", "", text).strip()
    return text.strip()


def parse_questions(pdf_path: str) -> list[dict]:
    questions = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages[1:], 1):
            text = page.extract_text() or ""
            if not text.strip():
                continue

            section = get_section(page_num)
            words = page.extract_words(keep_blank_chars=True, x_tolerance=3, y_tolerance=3)
            curves = page.curves

            q_circles = [
                c for c in curves
                if c.get("fill")
                and len(c.get("non_stroking_color", ())) >= 2
                and abs(c["non_stroking_color"][1] - CORRECT_COLOR_G) < 0.05
                and c["width"] > 10
                and c["x0"] < 85
            ]
            opt_circles = sorted(
                [c for c in curves if 12 < c["width"] < 15 and 80 < c["x0"] < 90],
                key=lambda c: c["top"],
            )
            sel_circles = [c for c in curves if is_inner_circle(c) and 85 < c["x0"] < 95]

            if not q_circles or not opt_circles:
                continue

            for qc in q_circles:
                q_words = [
                    w for w in words
                    if qc["top"] - 5 <= w["top"] <= qc["top"] + 100 and w["x0"] > qc["x1"]
                ]
                q_words.sort(key=lambda w: (round(w["top"] / 8) * 8, w["x0"]))
                q_text = clean_question_text(" ".join(w["text"] for w in q_words))

                next_q_tops = [c["top"] for c in q_circles if c["top"] > qc["top"]]
                next_q_top = min(next_q_tops) if next_q_tops else float("inf")
                my_opts = [c for c in opt_circles if qc["top"] < c["top"] < next_q_top]

                options, correct_idx = [], None
                for oc in my_opts:
                    opt_words = [
                        w for w in words
                        if abs(w["top"] - oc["top"]) < 15 and w["x0"] > oc["x1"]
                    ]
                    opt_words.sort(key=lambda w: w["x0"])
                    opt_text = " ".join(w["text"] for w in opt_words).strip()
                    is_selected = any(abs(sc["top"] - oc["top"]) < 10 for sc in sel_circles)
                    if opt_text:
                        options.append(opt_text)
                        if is_selected:
                            correct_idx = len(options) - 1

                if q_text and len(options) >= 2:
                    questions.append({
                        "question": q_text,
                        "options": options,
                        "correct": correct_idx if correct_idx is not None else 0,
                        "section": section,
                    })

    return questions


# Manually add questions that were missed by the parser
# (e.g. the one wrong answer in the test — no filled circle to detect)
MANUAL_QUESTIONS = [
    {
        # Page 34: first question of ЦИВІЛЬНЕ ПРОЦЕСУАЛЬНЕ ПРАВО
        # Score was 0/1 (wrong answer) so no inner circle — parser skipped it
        "insert_after_index": 50,  # after last ЦИВІЛЬНЕ ПРАВО question (0-based)
        "question": "За 2 дні до судового засідання позивач подав до місцевого суду заяву про відвід судді. Який склад суду має розглянути цю заяву?",
        "options": [
            "Суддя, який розглядає справу",
            "Суддя, який не входить до складу суду, що розглядає справу",
            "Суд відповідної інстанції, найбільш територіально наближений до цього суду",
            "Суд апеляційної інстанції",
        ],
        "correct": 0,
        "section": "ЦИВІЛЬНЕ ПРОЦЕСУАЛЬНЕ ПРАВО",
    },
]


def parse_questions_with_manual(pdf_path: str) -> list[dict]:
    questions = parse_questions(pdf_path)
    for mq in sorted(MANUAL_QUESTIONS, key=lambda x: x["insert_after_index"]):
        idx = mq["insert_after_index"] + 1
        questions.insert(idx, {
            "question": mq["question"],
            "options": mq["options"],
            "correct": mq["correct"],
            "section": mq["section"],
        })
    return questions


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "test.pdf"
    qs = parse_questions_with_manual(path)
    print(json.dumps(qs, ensure_ascii=False, indent=2))
    print(f"\nTotal: {len(qs)} questions", file=sys.stderr)
