#ifndef CHECK_H
#define CHECK_H

/*
 * Minimal reporting framework shared by every test suite.
 *
 * Goal: output that explains itself. Each section states WHAT it checks, and
 * the footer states WHICH suite ran, how many checks passed and whether any
 * failed. The exit code is ready for make / CI.
 */

#include <stdio.h>
#include <stdlib.h>

static int g_checks   = 0;
static int g_failures = 0;

#define CHECK(cond) do {                                                          \
        g_checks++;                                                               \
        if (cond) {                                                               \
            printf("  ✔ %s\n", #cond);                                            \
        } else {                                                                  \
            g_failures++;                                                         \
            printf("  ✘ FALLA %s:%d  %s\n", __FILE__, __LINE__, #cond);           \
        }                                                                         \
    } while (0)

/* Section header: what is being tested now and why it matters.
 * __attribute__((unused)) avoids warnings in suites with few sections. */
static void check_section(const char *title, const char *explains)
    __attribute__((unused));
static void check_section(const char *title, const char *explains)
{
    printf("\n── %s · %s\n", title, explains);
}

/* Suite header: printed first, so it is clear what is being looked at. */
static void check_header(const char *suite, const char *explains)
    __attribute__((unused));
static void check_header(const char *suite, const char *explains)
{
    printf("═══ %s · %s\n", suite, explains);
}

/* Footer: readable result + correct exit code. */
static int check_summary(const char *suite)
    __attribute__((unused));
static int check_summary(const char *suite)
{
    int passed = g_checks - g_failures;
    if (g_failures == 0) {
        printf("\n✔ %s: %d de %d checks pasaron — todo verde\n", suite, passed, g_checks);
        return EXIT_SUCCESS;
    }
    printf("\n✘ %s: %d de %d checks pasaron — %d FALLO(S)\n", suite, passed, g_checks, g_failures);
    return EXIT_FAILURE;
}

#endif /* CHECK_H */
