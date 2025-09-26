import json
import argparse
import sys


def process_tests(data, pattern, output_file):
    lines = []
    lines.append(f"|{pattern}, Name with Url|Count|\n")
    lines.append("| -------- | ------- |\n")

    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue
        if not item["name"].endswith(pattern):
            continue

        parts = item["artifact"].split("--")
        if len(parts) > 0 and len(parts[1]) > 0 and len(parts[2]) > 0:
            p2 = parts[2]
            if p2 == "unknown":
                job_url = (
                    f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
                )
            else:
                job_url = f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}/job/{p2}"
        else:
            job_url = item["artifact"]

        lines.append(f"|[{item['name']}]({job_url})|{item['failure_count']}|\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)


def process_crash(data, pattern, output_file):
    lines = []
    lines.append(f"|{pattern}, Name with Url|Count|\n")
    lines.append("| -------- | ------- |\n")

    for item in data:
        if "crash" in item["name"]:
            job_url = ""
            parts = item["artifact"].split("--")
            if len(parts) > 0 and len(parts[1]) > 0:
                job_url = (
                    f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
                )
            else:
                job_url = item["artifact"]
            lines.append(f"|[{item['name']}]({job_url})|{item['failure_count']}|\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)


def process_flaky(data, output_file):
    transformed = []
    for item in data:
        name_parts = item["name"].split("/")
        if len(name_parts) < 2:
            continue
        parts = item["artifact"].split("--")
        if len(parts) > 0 and len(parts[1]) > 0 and len(parts[2]) > 0:
            p2 = parts[2]
            if p2 == "unknown":
                job_url = (
                    f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}"
                )
            else:
                job_url = f"https://github.com/temporalio/temporal/actions/runs/{parts[1]}/job/{p2}"
        else:
            job_url = item["artifact"]

        transformed.append(
            {
                "name": item["name"],  # the 'name' field from the original
                "job_url": job_url,  # the newly constructed job URL
                "count": item["failure_count"],  # store the failure_count
            }
        )

        if len(transformed) > 10:
            break

    # Write both markdown table (for GitHub) and plain text (for Slack)
    lines = []
    lines.append("|Name with Url|Count|\n")
    lines.append("| -------- | ------- |\n")
    for item in transformed:
        lines.append(f"|[{item['name']}]({item['job_url']})|{item['count']}|\n")

    with open(output_file, "w") as outfile:
        outfile.writelines(lines)
    
    # Also create a plain text version for Slack
    slack_file = output_file.replace('.txt', '_slack.txt')
    slack_lines = []
    for item in transformed:
        slack_lines.append(f"• {item['name']} - {item['count']} failures\n")
    
    with open(slack_file, "w") as outfile:
        outfile.writelines(slack_lines)


def process_json_file(input_filename):
    with open(input_filename, "r") as file:
        # Load the file content as JSON
        data = json.load(file)

    process_flaky(data, "flaky.txt")
    process_tests(data, "(timeout)", "timeout.txt")
    process_tests(data, "(retry 2)", "retry.txt")
    process_crash(data, "(crash)", "crash.txt")


def main():
    parser = argparse.ArgumentParser(description='Process flaky test data')
    parser.add_argument('--file', '-f', default='out7.json', 
                       help='Input JSON file to process (default: out7.json)')
    
    args = parser.parse_args()
    
    try:
        process_json_file(args.file)
        print(f"Successfully processed {args.file}")
    except FileNotFoundError:
        print(f"Error: File {args.file} not found", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {args.file}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error processing {args.file}: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
