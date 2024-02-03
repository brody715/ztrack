import ztracker


def main():
    with ztracker.create(
        result_dir=f"{ztracker.str_cwd()}/tracks-{ztracker.str_datetime()}"
    ) as tracker:
        data = b""

        tracker = tracker.with_settings(reporter="ai", meta=dict(step=1, epoch=1))

        tracker.track(
            {
                "name": "hello, world",
                "image-2": ztracker.Artifact(
                    data, format="png", save_func=lambda data, path: cv2.imwrite(path)
                ),
            },
            commit=False,
        )

        tracker.track(
            {
                "models": ztracker.Artifact(
                    data, save_fn=lambda data, path: torch.save(data, path)
                )
            }
        )

        tracker.track({"name": "hello, world"}, commit=True)

        # Tracing #
        with tracker.span("span_name"):
            tracker.trace({})
            pass

        tracker.with_fields({"name": "asdf", "image_1": ztracker.Image()}).info(
            "hello, world", exclude_fields=["image-1"]
        ).track()

        # Logs #
        tracker.debug("hello, world")
        tracker.info("name")
        tracker.warn("name")
        tracker.error("name")

        # Metrics #
        tracker.counter("123").add()
        tracker.gauce("455")
        tracker.summary("summary")
        tracker.histogram("asdf")
