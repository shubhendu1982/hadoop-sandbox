from mrjob.job import MRJob
from mrjob.step import MRStep

class PlatformByGender(MRJob):

    def mapper(self, _, line):
        # Skip the header
        if line.startswith("User_ID"):
            return

        fields = line.split(',')
        if len(fields) != 10:
            return

        user_id, age, gender, platform, daily_usage, posts_per_day, likes_received, comments_received, messages_sent, dominant_emotion = fields

        yield (gender, platform), 1

    def reducer(self, key, values):
        gender, platform = key
        yield (gender, platform), sum(values)

if __name__ == '__main__':
    PlatformByGender.run()

