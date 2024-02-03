import ztracker
import logging

logging.basicConfig(level=logging.INFO)


def handle_write_tensorboard(evt: ztracker.Event, reporter: str):
    if evt.get_meta("tb"):
        print(evt)


def main():
    with ztracker.create(result_dir=".local/tracks-1") as tracker:
        data = "hello, world"
        tracker.register_event_callback(handle_write_tensorboard)

        for epoch in range(100):
            for idx in range(100):
                tracker = tracker.with_settings(
                    meta=dict(step=epoch * 100 + idx, epoch=epoch)
                )
                tracker.track(
                    {
                        "name": "hello, world",
                        "train/loss": 123,
                        # 'image-2': tracker.Artifact(data, format='txt', prefix='h', save_func=lambda data, path: path.write_text(data)),
                    },
                    commit=False,
                )

                tracker.track(
                    {
                        # 'models': tracker.Artifact('resnet.models', format='pth', save_func=lambda data, path: path.write_text(data)),
                    },
                    commit=False,
                )

                tracker.track({"name": "hello, world"}, commit=False)

                tracker.with_fields(
                    {
                        "name": "asdf",
                        "age": 1,
                        "idx": idx,
                    }
                ).debug("hello, world", exclude_fields=["image-1"]).track()

                tracker.track({"train/loss": 1.23}, meta={"tb": True})


if __name__ == "__main__":
    main()
