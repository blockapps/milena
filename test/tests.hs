{-# LANGUAGE OverloadedStrings #-}

module Main where

import Prelude

-- base
import Control.Monad (void)
import Data.Either (isRight, isLeft)
import qualified Data.List.NonEmpty as NE

-- Hackage
import Control.Lens
import Control.Monad.Except (catchError, throwError)
import Control.Monad.Trans (liftIO)
import Test.Tasty
import Test.Tasty.Hspec
import Test.Tasty.QuickCheck
import qualified Data.ByteString.Char8 as B

-- local
import Network.Kafka
import Network.Kafka.Consumer
import Network.Kafka.Producer
import Network.Kafka.Protocol



main :: IO ()
main = testSpec "milena" tests >>= defaultMain

run = runKafka $ mkKafkaState "milena-test-client" ("localhost", 9092)

tests :: Spec
tests = do
    describe "basic create topic, write/read message, delete topic" $ testCreateDeleteTopics "milena-test"
    describe "write/read messages" $ testReadWriteMessages "milena-write-test"
    describe "exceptions" testExceptions
    describe "metadata" testMetadata
    describe "heartbeat" testHeartbeat
    describe "commit offsets" $ testCommitOffsets "commit-offset"

testCreateDeleteTopics :: TopicName -> Spec
testCreateDeleteTopics topicName = do
    describe "create topic(s)" $ do
        it "create topic non existent topic" $ do
            result <- run $ do
                createTopic (createTopicsRequest topicName 1 1 [] [])
            result `shouldBe` (Right $ TopicsResp [(topicName, NoError)])

        it "create existing topic" $ do
            result <- run $ do
                createTopic (createTopicsRequest topicName 1 1 [] [])
            result `shouldBe` (Right $ TopicsResp [(topicName, TopicAlreadyExists)])

    describe "delete topic(s)" $ do
        it "delete topic existing topic" $ do
            result <- run $ do
                deleteTopic (deleteTopicsRequest topicName)
            result `shouldBe` (Right $ DeleteTopicsResp [(topicName, NoError)])

        it "delete non-existing topic" $ do
            let nonexistentTopicName = "this-topic-shouldnt-exit"
            result <- run $ do
                deleteTopic (deleteTopicsRequest nonexistentTopicName)
            result `shouldBe` (Right $ DeleteTopicsResp [(nonexistentTopicName, UnknownTopicOrPartition)])


testReadWriteMessages :: TopicName -> Spec
testReadWriteMessages topicName =
    Test.Tasty.Hspec.beforeAll (createTestTopic topicName) $ Test.Tasty.Hspec.afterAll_ (deleteTestTopic topicName) $ do

        let byteMessages = fmap (TopicAndMessage topicName . makeMessage . B.pack)
            requireAllAcks = do
                stateRequiredAcks .= -1
                stateWaitSize .= 1
                stateWaitTime .= 1000


        describe "can talk to local Kafka server" $ do
            prop "can produce messages" $ \ms -> do
                result <- run . produceMessages $ byteMessages ms
                result `shouldSatisfy` isRight

            prop "can produce compressed messages" $ \ms -> do
                result <- run . produceCompressedMessages Gzip $ byteMessages ms
                result `shouldSatisfy` isRight

            prop "can produce multiple messages" $ \(ms, ms', ms'') -> do
                result <- run $ do
                    r1 <- produceMessages $ byteMessages ms
                    r2 <- produceMessages $ byteMessages ms'
                    r3 <- produceCompressedMessages Gzip $  byteMessages ms''
                    return $ r1 ++ r2 ++ r3
                result `shouldSatisfy` isRight

        prop "can fetch messages" $ do
            result <- run $ do
                offset <- getLastOffset EarliestTime 0 topicName
                withAnyHandle (\handle -> fetch' handle =<< fetchRequest offset 0 topicName)
            result `shouldSatisfy` isRight

        prop "can roundtrip messages" $ \ms key -> do
            let messages = byteMessages ms
            result <- run $ do
                requireAllAcks
                info <- brokerPartitionInfo topicName

                case getPartitionByKey (B.pack key) info of
                    Just PartitionAndLeader { _palLeader = leader, _palPartition = partition } -> do
                        let _payload = [(TopicAndPartition topicName partition, groupMessagesToSet NoCompression messages)]
                            s = stateBrokers . at leader
                        [(_topicName, [(_, NoError, offset)])] <- _produceResponseFields <$> send leader _payload
                        broker <- findMetadataOrElse [topicName] s (KafkaInvalidBroker leader)
                        resp <- withBrokerHandle broker (\handle -> fetch' handle =<< fetchRequest offset partition topicName)
                        return $ fmap tamPayload . fetchMessages $ resp

                    Nothing -> fail "Could not deduce partition"

            result `shouldBe` Right (tamPayload <$> messages)

        prop "can roundtrip compressed messages" $ \(NonEmpty ms) -> do
            let messages = byteMessages ms
            result <- run $ do
                requireAllAcks
                produceResps <- produceCompressedMessages Gzip messages

                case map _produceResponseFields produceResps of
                    [[(_topicName, [(partition, NoError, offset)])]] -> do
                        resp <- fetch offset partition topicName
                        return $ fmap tamPayload . fetchMessages $ resp

                    _ -> fail "Unexpected produce response"

            result `shouldBe` Right (tamPayload <$> messages)


        prop "can roundtrip keyed messages" $ \(NonEmpty ms) key -> do
            let _keyBytes = B.pack key
                messages = fmap (TopicAndMessage topicName . makeKeyedMessage _keyBytes . B.pack) ms

            result <- run $ do
                requireAllAcks
                produceResps <- produceMessages messages

                case map _produceResponseFields produceResps of
                    [[(_topicName, [(partition, NoError, offset)])]] -> do
                        resp <- fetch offset partition topicName
                        return $ fmap tamPayload . fetchMessages $ resp

                    _ -> fail "Unexpected produce response"

            result `shouldBe` Right (tamPayload <$> messages)


testExceptions :: Spec
testExceptions =
    describe "withAddressHandle" $ do
        it "turns 'IOException's into 'KafkaClientError's" $ do
            result <- run $ withAddressHandle ("localhost", 9092) (\_ -> liftIO $ ioError $ userError "SOMETHING WENT WRONG!") :: IO (Either KafkaClientError ())
            result `shouldSatisfy` isLeft

        it "discards monadic effects when exceptions are thrown" $ do
            result <- run $ do
                stateName .= "expected"
                _ <- flip catchError (return . Left) $ withAddressHandle ("localhost", 9092) $ \_ -> do
                    stateName .= "changed"
                    _ <- throwError KafkaFailedToFetchMetadata
                    n <- use stateName
                    return (Right n)
                use stateName
            result `shouldBe` Right "expected"

testMetadata :: Spec
testMetadata = do
    describe "updateMetadatas" $ do
        it "de-dupes _stateAddresses" $ do
            result <- run $ do
                stateAddresses %= NE.cons ("localhost", 9092)
                updateMetadatas []
                use stateAddresses
            result `shouldBe` fmap NE.nub result

testHeartbeat :: Spec
testHeartbeat = do
    it "returns unknown member id " $ do
        result <- run $ do
            heartbeat (heartbeatRequest "non-existent-group-id" 143 "fake-member-id")
        result `shouldBe` (Right $ HeartbeatResp UnknownMemberId)


testCommitOffsets :: TopicName -> Spec
testCommitOffsets topicName = do
    Test.Tasty.Hspec.afterAll_ (deleteTestTopic topicName) $ do
        describe "can commit messages" $ do
            it "create a topic" $ do
                topicCreation <- run $ do
                    createTopic (createTopicsRequest topicName 3 1 [] [])
                topicCreation `shouldBe` (Right $ TopicsResp [(topicName, NoError)])

        it "commit offset 5 to partition 0 for consumer group \"group1\"" $ do
            commitOff <- run $ do
                commitOffset (commitOffsetRequest (ConsumerGroup "group1") topicName 0 (Offset 5))
            commitOff `shouldBe` Right (OffsetCommitResp [(topicName,[(Partition 0,NoError)])])

        it "commit offset 15 to partition 1 for consumer group \"group1\"" $ do
            commitOff <- run $ do
                commitOffset (commitOffsetRequest (ConsumerGroup "group1") topicName 1 (Offset 15))
            commitOff `shouldBe` Right (OffsetCommitResp [(topicName,[(Partition 1,NoError)])])

        it "commit offset 10 to partition 2 for consumer group \"group2\"" $ do
            commitOff <- run $ do
                commitOffset (commitOffsetRequest (ConsumerGroup "group2") topicName 2 10)
            commitOff `shouldSatisfy` isRight

        it "fetch offset from partition 0 for \"group1\"" $ do
            fetchOff <- run $ do
                fetchOffset (fetchOffsetRequest (ConsumerGroup "group1") topicName 0)
            fetchOff `shouldBe` Right (OffsetFetchResp [(topicName,[(Partition 0, Offset 5,Metadata (KString {_kString = ""}),NoError)])])

        it "fetch offset from partition 0 for \"group2\"" $ do
            fetchOff <- run $ do
                fetchOffset (fetchOffsetRequest (ConsumerGroup "group2") topicName 0)
            fetchOff `shouldBe` Right (OffsetFetchResp [(topicName,[(Partition 0, Offset (-1),Metadata (KString {_kString = ""}),UnknownTopicOrPartition)])])

        it "note that getLastOffset is unchanged" $ do
            getLastOff <- run $ do
                getLastOffset EarliestTime 0 topicName
            getLastOff `shouldSatisfy` isRight
            getLastOff `shouldBe` (Right $ Offset 0)



prop :: Testable prop => String -> prop -> SpecWith ()
prop s = it s . property

createTestTopic :: TopicName -> IO ()
createTestTopic topicName = do
    void $ run $ do
        createTopic (createTopicsRequest topicName 1 1 [] [])

deleteTestTopic :: TopicName -> IO ()
deleteTestTopic topicName = do
    void $ run $ do
        stateAddresses %= NE.cons ("localhost", 9092)
        deleteTopic (deleteTopicsRequest topicName)
