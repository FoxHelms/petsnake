from kafka import KafkaConsumer, TopicPartition
import json, os, threading
from dotenv import load_dotenv
import pygame
import random
import sys
import queue


load_dotenv()

kafka_server = str(os.getenv("KAFKA_SERVER", "Kafka Server not found"))

TOPIC = "room.temp"
BOOTSTRAP_SERVERS = [kafka_server + ":9092"]

def kafka_consumer_thread(q):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
    print("Consumer started")
    print(consumer.bootstrap_connected())
    
    for msg in consumer:
        q.put(msg.value["temp_f"])

def snake_game(q):
    # === CONFIG ===
    CELL_SIZE = 20
    GRID_WIDTH, GRID_HEIGHT = 30, 20
    SCREEN_WIDTH, SCREEN_HEIGHT = CELL_SIZE * GRID_WIDTH, CELL_SIZE * GRID_HEIGHT
    FPS = 60

    # === COLORS ===
    WHITE = (255, 255, 255)
    GREEN = (0, 255, 0)
    DARK_GREEN = (0, 155, 0)
    RED = (255, 0, 0)
    BLACK = (0, 0, 0)

    # === INIT ===
    pygame.init()
    screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
    clock = pygame.time.Clock()
    font = pygame.font.SysFont("Arial", 36) 

    # === SNAKE & FOOD SETUP ===
    snake = [(5, 5), (4, 5), (3, 5)]
    direction = (1, 0)
    food = (random.randint(0, GRID_WIDTH - 1), random.randint(0, GRID_HEIGHT - 1))

    def draw_cell(pos, color):
        x, y = pos
        rect = pygame.Rect(x * CELL_SIZE, y * CELL_SIZE, CELL_SIZE, CELL_SIZE)
        pygame.draw.rect(screen, color, rect)

    def move_snake(snake, direction):
        head_x, head_y = snake[0]
        dx, dy = direction
        new_head = (head_x + dx, head_y + dy)
        return [new_head] + snake[:-1]

    def check_collision(snake):
        head = snake[0]
        if (head in snake[1:] or
            not 0 <= head[0] < GRID_WIDTH or
            not 0 <= head[1] < GRID_HEIGHT):
            return True
        return False

    def handle_input(current_dir):
        keys = pygame.key.get_pressed()
        dx, dy = current_dir
        if keys[pygame.K_UP] and dy == 0:
            return (0, -1)
        if keys[pygame.K_DOWN] and dy == 0:
            return (0, 1)
        if keys[pygame.K_LEFT] and dx == 0:
            return (-1, 0)
        if keys[pygame.K_RIGHT] and dx == 0:
            return (1, 0)
        return current_dir

    while True:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()

        currTemp = q.get()
        text_surface = font.render(f"Sensor Output: {currTemp}", True, (255, 255, 255))
        text_rect = text_surface.get_rect(center=(SCREEN_WIDTH // 2, SCREEN_HEIGHT // 2))

        direction = handle_input(direction)

        # Move snake
        new_head = (snake[0][0] + direction[0], snake[0][1] + direction[1])
        snake.insert(0, new_head)

        # Check for food
        if new_head == food:
            food = (random.randint(0, GRID_WIDTH - 1), random.randint(0, GRID_HEIGHT - 1))
        else:
            snake.pop()

        # Check collision
        if check_collision(snake):
            print("Game Over!")
            pygame.quit()
            sys.exit()

        # Draw
        screen.fill(BLACK)
        screen.blit(text_surface, text_rect)
        for segment in snake:
            draw_cell(segment, GREEN)
        draw_cell(food, RED)
        pygame.display.flip()

        cached_temp = currTemp
        clock.tick(FPS)








if __name__ == "__main__":
    q = queue.Queue()
    q.put(70.00)
    consumer_thread = threading.Thread(target=kafka_consumer_thread, args=(q,))
    consumer_thread.daemon = True
    consumer_thread.start()
    snake_game(q)

    print("All threads closed")
    

        

